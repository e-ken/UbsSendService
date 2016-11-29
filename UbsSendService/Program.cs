using System;
using System.Collections.Generic;
using System.ServiceProcess;
using System.Text;

namespace UbsSendService
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            if (args.Length > 0 && RunCommand(args)) return; 

            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[] 
			{ 
				new UbsSendService() 
			};
            ServiceBase.Run(ServicesToRun);
        }

        static bool RunCommand(string[] args)
        {
            if (!string.IsNullOrEmpty(args[0]) && args[0].Equals("/i", StringComparison.OrdinalIgnoreCase))
            {
                string userName = null, password = null;

                if (args.Length > 1) userName = args[1];
                if (args.Length > 2) password = args[2];
                
                ProjectInstaller.InstallService(userName, password);

                return true;
            }
            else if (!string.IsNullOrEmpty(args[0]) && args[0].Equals("/r", StringComparison.OrdinalIgnoreCase))
            {
                ProjectInstaller.RemoveService();
                return true;
            }
            else
            {
                Console.WriteLine("UbsSendService поддерживает следующие команды");
                Console.WriteLine("    /i [Имя] [Пароль] установить службу");
                Console.WriteLine("    /r удалить службу");
            }
            return false;
        }
    }
}
