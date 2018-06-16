using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Threading;
using System.IO;
using System.Web.Script.Serialization;
using System.Data;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Net;

namespace PHDProxyServer
{
    class Program
    {
        static TcpListener listen;
        static DateTime javaDateStart = new DateTime(1970, 1, 1, 0, 0, 0);
        static JavaScriptSerializer serializer = new JavaScriptSerializer();
        static int TAGS_MAX_COUNT = 20;
        static int SERVER_PORT = 9001;

        private static void Usage()
        {
            string help = @"
################################### ProxyServer Usag  #############################################
#                                                                                                 #
#            ProxyServer Usage:                                                                   #
#                                                                                                 #
#                -c : client mode mainly for test or debug                                        #
#                -s : server mode                                                                 # 
#                                                                                                 #                
#            In client mode, the following options could be used:                                 #
#                                                                                                 #
#                -opc/phd : data source type, default is phd                                      #            
#                -ping/browse/fetch   :  request type,                                            #  
#                -debug : print responsed lines's detail than just count number                   # 
#                -hostname : opc/phd server's hostname                                            #
#                -servername : opc server name                                                    # 
#                -tagname : filter patten with tag name when browsing                             #
#                -datatype : filter patten with data type when browsing                           #
#                -stype : only valid for phd source                                               #
#                -sfreq : only valid for phd source                                               #
#                -rtype : only valid for phd source                                               #
#                -rfreq : only valid for phd source                                               #
#                -offset : only valid for phd source                                              #
#                -start : only valid for phd source                                               #
#                -end : only valid for phd source                                                 #
#                -tagsource : only valid for phd source                                           #
#                                                                                                 #            
#            Examples:                                                                            # 
#                                                                                                 #
#                   ProxyServer -c ping -hostname:9.0.0.1                                         #
#                   ProxyServer -c -type:browse -hostname:9.0.0.1                                 # 
#                                        -tagmodel:onlyname -tagname:* -count:100                 #   
#                   ProxyServer -c -type:fetch -hostname:9.0.0.1                                  #         
#                                                                                                 #
#                   ProxyServer -c -opc -ping -hostname:localhost                                 #
#                                             -servername:Matrikon.OPC.Simulation.1               #
#                                                                                                 #
#                                                                                                 #
#                                                                                                 #
#                                                                                                 #
###################################################################################################
            ";
            Console.Out.Write(help);
        }

        static void Main(string[] args)
        {
            if (HasOption(args, "c"))
            {
                if (HasOption(args, "opc"))
                    OPCProxy.SendRequest(args);
                else
                    PHDProxy.SendRequest(args);

                return;
            }
            else if (HasOption(args, "s"))
            {
                Task server = new Task(() =>
                {
                    listen = new TcpListener(System.Net.IPAddress.Parse("127.0.0.1"), SERVER_PORT);

                    listen.Start();
                    Console.Out.WriteLine("Server: start server .............................. \n");

                    while (true)
                    {
                        TcpClient client = listen.AcceptTcpClient();
                        Console.Out.WriteLine("Accept a new request !\n");

                        Task clientTask = new Task(() =>
                        {
                            HandleSession(client);
                        });

                        clientTask.Start();
                    }
                });

                server.Start();
                server.Wait();
            }
            else
            {
                Usage();
            }
        }

        private static void HandleSession(TcpClient tcpclient)
        {
            using (StreamReader reader = new StreamReader(tcpclient.GetStream()))
            {
                string header = reader.ReadLine();
                Dictionary<string, string> args = serializer.Deserialize<Dictionary<string, string>>(header);

                Console.Out.WriteLine("Request Header ： {0}", header);
                
                string source = null;
                if (args.ContainsKey("source"))
                    source = args["source"];
                if (source != null && source.Equals("opc"))
                {
                    new OPCProxy(tcpclient, reader, args).HandleRequest();
                }
                else
                {
                    PHDProxy.HandleRequest(tcpclient, reader, args);
                }

                Console.Out.WriteLine("###END###");
            }


            if (tcpclient.Connected)
            {
                tcpclient.Close();
            }
        }

        public static bool IsArgumentName(string argument, string name, bool isOption = false)
        {
            if (argument[0] == '-' && argument.Length > (name.Length + 2) && argument[name.Length + 1] == ':')
            {
                for (int i = 0; i < name.Length; ++i)
                {
                    if (argument[i + 1] != name[i])
                        return false;
                }
                return true;
            }
            return false;
        }

        public static bool IsOption(string argument, string name)
        {
            if (argument[0] == '-' && argument.Length == (name.Length + 1))
            {
                for (int i = 0; i < name.Length; ++i)
                {
                    if (argument[i + 1] != name[i])
                        return false;
                }
                return true;
            }
            return false;
        }

        public static void SafeGet(Dictionary<string, string> args, string key, ref string value)
        {
            if (args.ContainsKey(key) && !string.IsNullOrEmpty(args[key]))
            {
                value = args[key];
            }
        }

        public static string GetArgument(string[] args, string name, string defaultValue = null)
        {
            for (int i = 0; i < args.Length; ++i)
            {
                if (IsArgumentName(args[i], name))
                    return args[i].Substring(name.Length + 2);
            }
            return defaultValue;
        }

        public static bool HasOption(string[] args, string name)
        {
            for (int i = 0; i < args.Length; ++i)
            {
                if (IsOption(args[i], name))
                    return true;
            }
            return false;
        }

        public static void WriteLine(Socket socket, string message)
        {
            byte[] buffer = System.Text.Encoding.ASCII.GetBytes(message + "\n");
            socket.Send(buffer);
        }

        
    }
}
