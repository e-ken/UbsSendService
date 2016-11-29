using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;

using System.ServiceProcess;

namespace UbsSendService
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();
        }

        public ServiceProcessInstaller ServiceProcessInstaller
        {
            get
            {
                return this.serviceProcessInstaller;
            }
        }

        public static void InstallService(string userName, string password)
        {
            using (ProjectInstaller pi = new ProjectInstaller())
            {
                IDictionary savedState = new Hashtable();
                try
                {
                    pi.ServiceProcessInstaller.Username = userName;
                    pi.ServiceProcessInstaller.Password = password;

                    pi.Context = new InstallContext();
                    pi.Context.Parameters.Add("assemblypath", System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName);
                    foreach (Installer i in pi.Installers)
                        i.Context = pi.Context;
                    pi.Install(savedState);
                    pi.Commit(savedState);

                    foreach (ServiceController sc in System.ServiceProcess.ServiceController.GetServices())
                    {
                        if (sc.ServiceName.Equals("UbsSendService", StringComparison.OrdinalIgnoreCase))
                        {
                            if (sc.Status == ServiceControllerStatus.Stopped) sc.Start();
                            sc.WaitForStatus(ServiceControllerStatus.Running, new TimeSpan(0, 2, 0));
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    pi.Rollback(savedState);
                }
            }
        }
        public static void RemoveService()
        {
            foreach (ServiceController sc in System.ServiceProcess.ServiceController.GetServices())
            {
                if (sc.ServiceName.Equals("UbsSendService", StringComparison.OrdinalIgnoreCase))
                {
                    if (sc.Status == ServiceControllerStatus.Running) sc.Stop();
                    sc.WaitForStatus(ServiceControllerStatus.Stopped, new TimeSpan(0, 2, 0));
                    break;
                }
            }

            using (ProjectInstaller pi = new ProjectInstaller())
            {
                try
                {
                    pi.Context = new InstallContext();
                    pi.Context.Parameters.Add("assemblypath", System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName);
                    foreach (Installer i in pi.Installers)
                        i.Context = pi.Context;
                    pi.Uninstall(null);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
    }
}
