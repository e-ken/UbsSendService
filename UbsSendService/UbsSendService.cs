using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;
using System.IO;
using System.Xml;
using System.Threading;
using System.Data.SqlClient;
using UbsService;
using System.Configuration;

namespace UbsSendService
{
    public partial class UbsSendService : ServiceBase
    {
        private const int timeLoopWait = 200;
        private const int timeWaitCommand = 1000 * 60 * 10;
        private const int additionalTime = 5000;

        public UbsSendService()
        {
            InitializeComponent();
        }
        protected override void OnStart(string[] args)
        {
            try
            {
                this.RequestAdditionalTime(additionalTime);
                string connectionString = null;
                ConnectionStringSettings cSSettings = ConfigurationManager.ConnectionStrings["UbsSendConnectionString"];
                if (cSSettings == null || string.IsNullOrEmpty(cSSettings.ConnectionString))
                {
                    this.EventLog.WriteEntry("Строка соединения не задана в конфигурационном файле");
                    this.RequestAdditionalTime(additionalTime);
                    this.Stop();
                    return;
                }
                else
                    connectionString = cSSettings.ConnectionString;
                int intervalInSec = 0;
                if (!(int.TryParse(ConfigurationManager.AppSettings["Интервал сканирования"], out intervalInSec) && intervalInSec > 0))
                    intervalInSec = 300;

                string virtualFolder = ConfigurationManager.AppSettings["Виртуальный каталог"];
                if (string.IsNullOrEmpty(virtualFolder))
                {
                    this.EventLog.WriteEntry("Виртуальный каталог не задан в настройках");
                    this.RequestAdditionalTime(additionalTime);
                    this.Stop();
                    return;
                }
                
                string rootFolderIn = ConfigurationManager.AppSettings["Корневой каталог - входящие файлы"],
                    rootFolderOut = ConfigurationManager.AppSettings["Корневой каталог - исходящие файлы"];
                if (string.IsNullOrEmpty(rootFolderOut) || string.IsNullOrEmpty(rootFolderIn))
                {
                    this.EventLog.WriteEntry("Корневые каталоги не заданы в настройках");
                    this.RequestAdditionalTime(additionalTime);
                    this.Stop();
                    return;
                }
                
                try
                {
                    // Создаём дирректории
                    if (!Directory.Exists(rootFolderIn)) Directory.CreateDirectory(rootFolderIn);
                    if (!Directory.Exists(rootFolderOut)) Directory.CreateDirectory(rootFolderOut);
                }
                catch (Exception ex)
                {
                    this.EventLog.WriteEntry("Создание каталогов потерпело неудачу " + ex.Message);
                    this.RequestAdditionalTime(additionalTime);
                    this.Stop();
                    return;
                }

                
                int maxTryCount = 0;
                if (!(int.TryParse(ConfigurationManager.AppSettings["Порог попыток выгрузки файлов"], out maxTryCount) && maxTryCount > 0))
                    maxTryCount = 32767;
                
                Thread thread = new Thread(ThreadScanFiles);
                thread.IsBackground = true;
                thread.Start(new object[] { this.EventLog, connectionString, intervalInSec, virtualFolder, rootFolderIn, rootFolderOut, maxTryCount });
                
            }
            catch (Exception ex)
            {
                this.EventLog.WriteEntry(ex.ToString());
                this.Stop();
                return;
            }
        }
        protected override void OnStop()
        {

        }

        private static void ThreadScanFiles(object args)
        {
            
            object[] param = (object[])args;
            EventLog eventLog = (EventLog)param[0];
            string connectionString = (string)param[1];
            int intervalInSec = (int)param[2];
            string virtualFolder = (string)param[3],
                rootFolderIn = (string)param[4], rootFolderOut = (string)param[5];
            int maxTryCount = (int)param[6];

            try
            {
                List<Thread> listThreads = new List<Thread>();
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    while (true)
                    {
                        // Дождаться завершения работы всех потоков

                        if (!listThreads.Exists(new Predicate<Thread>(delegate(Thread match) { return match.IsAlive; })))
                        {
                            listThreads.Clear();

                            try
                            {
                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();
                                else if (connection.State == ConnectionState.Broken)
                                {
                                    connection.Close();
                                    connection.Open();
                                }
                            }
                            catch (Exception ex)
                            {
                                eventLog.WriteEntry(
                                    string.Format("Подключение к источнику данных провалилось{0}{1}",
                                    Environment.NewLine, ex.Message));
                            }

                            try
                            {
                                foreach (KeyValuePair<int, object[]> host in ScanTable(connection, maxTryCount))
                                {
                                    Thread thread = new Thread(ThreadSendFiles);
                                    thread.IsBackground = true;
                                    listThreads.Add(thread);
                                    thread.Start(new object[] { eventLog, virtualFolder, rootFolderIn, rootFolderOut, host.Value, connection});
                                }
                            }
                            catch (Exception ex)
                            {
                                eventLog.WriteEntry(
                                    string.Format("Создание потоков для обработки файлов потерпело неудачу{0}{1}",
                                    Environment.NewLine, ex.Message));
                            }
                            Thread.CurrentThread.Join(intervalInSec * 1000);
                        }
                        Thread.CurrentThread.Join(timeLoopWait);
                    }
                }
            }
            catch (Exception ex)
            {
                eventLog.WriteEntry(ex.ToString());
            }
        }

        private static void ThreadSendFiles(object args)
        {
            object[] param = (object[])args;
            EventLog eventLog = (EventLog)param[0];
            string virtualFolder = (string)param[1], rootFolderIn = (string)param[2], rootFolderOut = (string)param[3];
            object[] host = (object[])param[4];
            SqlConnection connection = (SqlConnection)param[5];
            
            string otherInfo =
                string.Format("Хост '{0}'{1}", host[2], Environment.NewLine) +
                string.Format("Порт '{0}'{1}", host[3], Environment.NewLine) +
                string.Format("DSN '{0}'{1}", host[4], Environment.NewLine) +
                string.Format("Пользователь '{0}'{1}", host[5], Environment.NewLine);// +
                //string.Format("Пароль '{0}'{1}", host[6], Environment.NewLine);
            
            try
            {
                using (UbsExecRPC execRPC = new UbsExecRPC())
                {
                    string pass = null;
                    try
                    {
                        UbsCrypto.DeCryptFromHexStr(";IUY o ;liuo &*( uyt", (string)host[6], out pass);
                        pass = pass.Substring(8);
                    }
                    catch (Exception ex)
                    {
                        eventLog.WriteEntry(string.Format("Расшифровка пароля заершилась неудачей", Environment.NewLine, ex.Message));
                        return;
                    }

                    try
                    {
                        Logon(execRPC, (string)host[2], (int)host[3], (string)host[4], (string)host[5], pass);
                    }
                    catch (Exception ex)
                    {
                        eventLog.WriteEntry(string.Format("Cоединение с хостом {0} на порт {1} потерпело неудачу{2}{3}",
                            host[2], host[3], Environment.NewLine, ex.Message));
                        return;
                    }
                    
                    foreach (object[] file in (List<object[]>)host[host.GetUpperBound(0)])
                    {
                        int idFile = (int)file[0];
                        short tryCount = (short)file[2]; tryCount++;
                        string pathIn = Path.Combine(rootFolderIn, (string)file[1]) + tryCount.ToString("_0000");
                        string pathOut = Path.Combine(rootFolderOut, (string)file[1]);
                        bool isOk = ProcessingFile(execRPC, pathIn, pathOut, idFile, tryCount, virtualFolder, otherInfo);
                        lock (connection)
                        {
                            try
                            {
                                if (isOk)
                                {
                                    using (SqlCommand command = new SqlCommand(
                                        "update MB_FILE_OUT set TRY_COUNT = @TRY_COUNT, STATE = 3, TIME_EDIT = @TIME_EDIT" +
                                        " where ID_FILE = @ID_FILE and STATE < 3", connection))
                                    {
                                        command.Parameters.Add(new SqlParameter("ID_FILE", idFile));
                                        command.Parameters.Add(new SqlParameter("TRY_COUNT", tryCount));
                                        command.Parameters.Add(new SqlParameter("TIME_EDIT", DateTime.Now));
                                        command.ExecuteNonQuery();
                                    }
                                }
                                else
                                {
                                    using (SqlTransaction transaction = connection.BeginTransaction())
                                    {
                                        using (SqlCommand command = new SqlCommand(
                                            "update MB_FILE_OUT set STATE = 1 where ID_FILE = @ID_FILE and STATE < 1;" +
                                            " update MB_FILE_OUT set TRY_COUNT = @TRY_COUNT, TIME_EDIT = @TIME_EDIT" +
                                            " where ID_FILE = @ID_FILE", connection, transaction))
                                        {
                                            command.Parameters.Add(new SqlParameter("ID_FILE", idFile));
                                            command.Parameters.Add(new SqlParameter("TRY_COUNT", tryCount));
                                            command.Parameters.Add(new SqlParameter("TIME_EDIT", DateTime.Now));
                                            command.ExecuteNonQuery();
                                        }
                                        int numTry = 1;
                                        using (SqlCommand command = new SqlCommand(
                                            "select {fn ifnull(max(NUM_TRY), 0)} + 1 from MB_FILE_OUT_LOG" +
                                            " where ID_FILE = @ID_FILE", connection, transaction))
                                        {
                                            command.Parameters.Add(new SqlParameter("ID_FILE", idFile));
                                            numTry = (int)command.ExecuteScalar();
                                        }
                                        using (SqlCommand command = new SqlCommand(
                                            "insert into MB_FILE_OUT_LOG(ID_FILE, NUM_TRY, NAME_FILE)" +
                                            " values(@ID_FILE, @NUM_TRY, @NAME_FILE)", connection, transaction))
                                        {
                                            command.Parameters.Add(new SqlParameter("ID_FILE", idFile));
                                            command.Parameters.Add(new SqlParameter("NUM_TRY", numTry));
                                            command.Parameters.Add(new SqlParameter("NAME_FILE", pathIn.Replace(rootFolderIn, "")));
                                            command.ExecuteNonQuery();
                                        }
                                        transaction.Commit();
                                    }
                                }
                                break;
                            }
                            catch (Exception ex)
                            {
                                eventLog.WriteEntry(string.Format("Запись в базу информации об обработки файла потерпело неудачу{0}{1}",
                                    Environment.NewLine, ex.Message));
                            }
                        }
                    }
                    try
                    {
                        Disconnect(execRPC);
                    }
                    catch (Exception ex)
                    {
                        eventLog.WriteEntry(string.Format("Разрыв соединение с хостом {0} порт {1} потерпело неудачу{2}{3}",
                            host[2], host[3], Environment.NewLine, ex.Message));
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                eventLog.WriteEntry(ex.ToString());
            }
        }

        private static void Logon(UbsExecRPC execRPC, string host, int port, string dsn, string user, string pass)
        {
            execRPC.set_NamedParameter("HostAddress", host);
            execRPC.set_NamedParameter("Port", port);

            object[,] arrParam = new object[3, 2];
            arrParam[0, 0] = "DSN"; arrParam[0, 1] = dsn;
            arrParam[1, 0] = "UserName"; arrParam[1, 1] = user;
            arrParam[2, 0] = "Password"; arrParam[2, 1] = pass;

            short status = 0;
            string message = null;
            bool isBreak = false;
            var respondEvent = new UbsExecRPC.FireRespondEventHandler(
                delegate(object sender, EventArgs args)
                {
                    status = (short)((UbsExecRPC)sender).get_NamedParameter("Status");
                    if ((status & 4) != 0) message = ((UbsExecRPC)sender).get_NamedParameter("TextError").ToString();
                    isBreak = true;
                });
            execRPC.Fire_Respond += respondEvent;
            execRPC.ExecCommand("Logon", arrParam);
            long count = 0;
            do { 
                Thread.CurrentThread.Join(timeLoopWait);
                count += timeLoopWait;
                if (count > timeWaitCommand) throw new System.TimeoutException("Время ожидания " + count.ToString() + " истекло");
            } while (!isBreak);
            execRPC.Fire_Respond -= respondEvent;
            if ((status & 4) != 0) throw new Exception(message);
        }
        private static void Disconnect(UbsExecRPC execRPC)
        {
            if (!string.IsNullOrEmpty((string)execRPC.get_NamedParameter("UserGUID")))
            {
                short status = 0;
                execRPC.ExecCommand("Disconnect", null);
                long count = 0;
                do
                {
                    status = (short)execRPC.get_NamedParameter("Status");
                    Thread.CurrentThread.Join(timeLoopWait);
                    count += timeLoopWait;
                    if (count > timeWaitCommand) throw new System.TimeoutException("Время ожидания " + count.ToString() + " истекло");
                } while ((status & 1) != 0);
            }
        }
        private static void PutFile(UbsExecRPC execRPC, string localPath, string virtualFolder)
        {
            object[,] arrParam = new object[3, 2];
            arrParam[0, 0] = "LocalFileName"; arrParam[0, 1] = localPath;
            arrParam[1, 0] = "SrvVirtualPatch"; arrParam[1, 1] = virtualFolder;
            arrParam[2, 0] = "SrvFileName"; arrParam[2, 1] = Path.GetFileName(localPath);

            short status = 0;
            string message = null;
            bool isBreak = false;
            var respondEvent = new UbsExecRPC.FireRespondEventHandler(
                delegate(object sender, EventArgs args)
                {
                    status = (short)((UbsExecRPC)sender).get_NamedParameter("Status");
                    if ((status & 4) != 0) message = ((UbsExecRPC)sender).get_NamedParameter("TextError").ToString();
                    isBreak = true;
                });
            execRPC.Fire_Respond += respondEvent;
            execRPC.ExecCommand("PutFile", arrParam);
            long count = 0;
            do { 
                Thread.CurrentThread.Join(timeLoopWait);
                count += timeLoopWait;
                if (count > timeWaitCommand) throw new System.TimeoutException("Время ожидания " + count.ToString() + " истекло");
            } while (!isBreak);
            execRPC.Fire_Respond -= respondEvent;
            if ((status & 4) != 0) throw new Exception(message);
        }
        private static void GetFile(UbsExecRPC execRPC, string localPath, string virtualFolder, string virtualFile)
        {
            object[,] arrParam = new object[3, 2];
            arrParam[0, 0] = "LocalFileName"; arrParam[0, 1] = localPath;
            arrParam[1, 0] = "SrvVirtualPatch"; arrParam[1, 1] = virtualFolder;
            arrParam[2, 0] = "SrvFileName"; arrParam[2, 1] = virtualFile;

            short status = 0;
            string message = null;
            bool isBreak = false;
            var respondEvent = new UbsExecRPC.FireRespondEventHandler(
                delegate(object sender, EventArgs args)
                {
                    status = (short)((UbsExecRPC)sender).get_NamedParameter("Status");
                    if ((status & 4) != 0) message = ((UbsExecRPC)sender).get_NamedParameter("TextError").ToString();
                    isBreak = true;
                });
            execRPC.Fire_Respond += respondEvent;
            execRPC.ExecCommand("GetFile", arrParam);
            long count = 0;
            do { 
                Thread.CurrentThread.Join(timeLoopWait);
                count += timeLoopWait;
                if (count > timeWaitCommand) throw new System.TimeoutException("Время ожидания " + count.ToString() + " истекло");
            } while (!isBreak);
            execRPC.Fire_Respond -= respondEvent;
            if ((status & 4) != 0) throw new Exception(message);
        }
        private static bool Run(UbsExecRPC execRPC, string outFileName, string virtualFolder, out string virtualFile)
        {
            virtualFile = null;
            
            UbsParam ubsParam = new UbsParam();
            ubsParam["Виртуальный каталог"] = virtualFolder;
            ubsParam["Имя входящего файла"] = outFileName;

            object[,] arrParam = new object[7, 2];
            arrParam[0, 0] = "TypeResource"; arrParam[0, 1] = "ASM";
            arrParam[1, 0] = "NameResource"; arrParam[1, 1] = @"UBS_ASM\MB\UbsMBExchange.dll->UbsBusiness.UbsMBLoad";
            arrParam[2, 0] = "NameFunction"; arrParam[2, 1] = "LoadFile";
            arrParam[3, 0] = "FormatInPutData"; arrParam[3, 1] = "XML";
            arrParam[4, 0] = "InPutData"; arrParam[4, 1] = ubsParam.ToXml();
            arrParam[5, 0] = "FormatOutPutData"; arrParam[5, 1] = "XML";
            arrParam[6, 0] = "AsynchronousOperation"; arrParam[6, 1] = false;

            short status = 0;
            string message = null;
            bool isBreak = false;
            var respondEvent = new UbsExecRPC.FireRespondEventHandler(
                delegate(object sender, EventArgs args)
                {
                    status = (short)((UbsExecRPC)sender).get_NamedParameter("Status");
                    if ((status & 4) != 0)
                        message = ((UbsExecRPC)sender).get_NamedParameter("TextError").ToString();
                    else if ((status & 2) != 0)
                    {
                        message = ((UbsExecRPC)sender).get_NamedParameter("TextError").ToString();
                        ubsParam.Parse(((UbsExecRPC)sender).get_NamedParameter("TextRespond").ToString());
                    }
                    isBreak = true;
                });
            execRPC.Fire_Respond += respondEvent;
            execRPC.ExecCommand("Run", arrParam);
            long count = 0;
            do { 
                Thread.CurrentThread.Join(timeLoopWait);
                count += timeLoopWait;
                if (count > timeWaitCommand) throw new System.TimeoutException("Время ожидания " + count.ToString() + " истекло");
            } while (!isBreak);
            execRPC.Fire_Respond -= respondEvent;
            if ((status & 4) != 0) throw new Exception(message);
            
            if (ubsParam.Contains("Имя исходящего файла"))
                virtualFile = (string)ubsParam["Имя исходящего файла"];

            if (ubsParam.Contains("Результат обработки") && ubsParam["Результат обработки"] != null)
                return (bool)ubsParam["Результат обработки"];

            throw new Exception("Результат обработки файла не известен");
        }

        private static bool ProcessingFile(UbsExecRPC execRPC, string pathIn, string pathOut, int idFile, int tryCount, string virtualFolder, string otherInfo)
        {
            StringBuilder protocol = new StringBuilder();
            
            protocol.AppendLine(string.Format("Подготовка файла '{0}' ID={1} к отправке", pathOut, idFile));
            protocol.AppendLine(string.Format("Попытка № {0}", tryCount));
            protocol.AppendLine(string.Format("Виртуальный каталог {0}", virtualFolder));
            protocol.AppendLine(string.Format("Путь входящий {0}", pathIn));
            protocol.AppendLine(string.Format("Путь исходящий {0}", pathOut));
            protocol.AppendLine(otherInfo);
            

            try
            {
                string folderIn = Path.GetDirectoryName(pathIn);
                if (!Directory.Exists(folderIn)) Directory.CreateDirectory(folderIn);

                if (!File.Exists(pathOut))
                {
                    protocol.AppendLine(string.Format("Файл '{0}' не найден", pathOut));
                }
                else
                {
                    try
                    {
                        PutFile(execRPC, pathOut, virtualFolder);
                        try
                        {
                            string virtualFile;
                            if (!Run(execRPC, Path.GetFileName(pathOut), virtualFolder, out virtualFile))
                            {
                                try
                                {
                                    // Обработка выполнена с ошибками (получен протокол с ошибками)
                                    if (!string.IsNullOrEmpty(virtualFile))
                                    {
                                        GetFile(execRPC, pathIn, virtualFolder, virtualFile);
                                        return false;
                                    }
                                    else
                                    {
                                        protocol.AppendLine(string.Format("Ошибка получения имени файла протокола обработки '{0}' ID={1}", pathOut, idFile));
                                    }
                                }
                                catch (Exception ex)
                                {
                                    protocol.AppendLine(string.Format("Ошибка получения протокола обработки файла '{0}' ID={1}", pathOut, idFile));
                                    protocol.AppendLine(ex.ToString());
                                }
                            }
                            else
                            {
                                // Обработка выполнена успешна (нет никаких ошибок)
                                return true;
                            }
                        }
                        catch (Exception ex)
                        {
                            protocol.AppendLine(string.Format("Ошибка обработки файла '{0}' ID={1}", pathOut, idFile));
                            protocol.AppendLine(ex.ToString());
                        }
                    }
                    catch (Exception ex)
                    {
                        protocol.AppendLine(string.Format("Ошибка передачи файла '{0}' ID={1}", pathOut, idFile));
                        protocol.AppendLine(ex.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                protocol.AppendLine(string.Format("Ошибка создания входящего каталога '{0}'", pathIn));
                protocol.AppendLine(ex.ToString());
            }
            // В процессе взаимодействия произошли ошибки
            using (StreamWriter sw = new StreamWriter(pathIn, false, Encoding.GetEncoding(1251)))
            {
                sw.Write(protocol.ToString());
            }

            return false;
        }

        private static Dictionary<int, object[]> ScanTable(SqlConnection connection, int maxTryCount)
        {
            //0 сформирован
            //1 ошибка обработки
            //2 отменен
            //3 обработан

            Dictionary<int, object[]> hosts = new Dictionary<int, object[]>();

            using (SqlCommand command = new SqlCommand(
                "update MB_FILE_OUT set STATE = 2, TIME_EDIT = @TIME_EDIT" +
                " where STATE < 2 and TRY_COUNT >= @MAX_TRY_COUNT", connection))
            {
                command.Parameters.Add(new SqlParameter("MAX_TRY_COUNT", maxTryCount));
                command.Parameters.Add(new SqlParameter("TIME_EDIT", DateTime.Now));
                command.ExecuteNonQuery();
            }

            using (SqlCommand command = new SqlCommand(
                "select h.ID_HOST, h.NAME_HOST, h.APPS_ADDRESS, h.APPS_PORT" +
                    ", h.DSN, h.UBS_USER_NAME, h.UBS_USER_PASSW" +
                    ", f.ID_FILE, f.NAME_FILE, f.TRY_COUNT" +
                " from MB_FILE_OUT f, MB_HOST h" +
                " where f.ID_HOST = h.ID_HOST and f.STATE < 2" +
                " order by h.ID_HOST, f.TIME_CREATE", connection))
            {
                using (SqlDataReader dr = command.ExecuteReader())
                {
                    List<object[]> files = null;
                    while (dr.Read())
                    {
                        int idHost = (int)dr["ID_HOST"];
                        if (!hosts.ContainsKey(idHost))
                        {
                            files = new List<object[]>();
                            hosts.Add(idHost, new object[] {
                                idHost,
                                dr["NAME_HOST"],
                                dr["APPS_ADDRESS"],
                                int.Parse((string)dr["APPS_PORT"]),
                                dr["DSN"],
                                dr["UBS_USER_NAME"],
                                dr["UBS_USER_PASSW"],
                                files});
                        }
                        files.Add(new object[] {
                            dr["ID_FILE"],
                            dr["NAME_FILE"],
                            dr["TRY_COUNT"]});
                    }
                }
            }
            return hosts;
        }
    }
}
