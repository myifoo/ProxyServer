using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Data;
using System.Web.Script.Serialization;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using OPCAutomation;
using System.Net;

namespace PHDProxyServer
{
    class OPCProxy
    {
        private TcpClient tcpclient;
        private StreamReader reader;
        private Dictionary<string, string> args;
        static JavaScriptSerializer serializer = new JavaScriptSerializer();
        static int SERVER_PORT = 9001;

        public OPCProxy(TcpClient tcpclient, StreamReader reader, Dictionary<string, string> args)
        {
            this.tcpclient = tcpclient;
            this.reader = reader;
            this.args = args;
        }
        
        public static void SendRequest(string[] args) 
        {
            using (TcpClient tcpclient = new TcpClient())
            {
                IPAddress address = IPAddress.Parse("127.0.0.1");
                IPEndPoint point = new IPEndPoint(address, SERVER_PORT);
                tcpclient.Connect(point);

                string hostname = Program.GetArgument(args, "hostname");
                string servername = Program.GetArgument(args, "servername");
                bool ping = Program.HasOption(args, "ping");
                bool browse = Program.HasOption(args, "browse");
                bool fetch = Program.HasOption(args, "fetch");
                bool debug = Program.HasOption(args, "debug");

                Dictionary<string, string> reqargs = new Dictionary<string, string>();
                reqargs.Add("hostname", hostname);
                reqargs.Add("servername", servername);
                reqargs.Add("source", "opc");

                //string tagname = Program.GetArgument(args, "tagname", "*");
                //string datatype = Program.GetArgument(args, "datatype", null);
                //string count = Program.GetArgument(args, "count", "100");

                if (fetch)
                {
                    string tagsource = Program.GetArgument(args, "tagsource", "browse"); // [browse, list, lines]
                    string list = Program.GetArgument(args, "list", null);

                    reqargs.Add("type", "fetch");
                    reqargs.Add("tagsource", tagsource);
                    reqargs.Add("list", list);
                }
                else if (browse)
                {
                    reqargs.Add("type", "browse");
                }
                else
                {
                    reqargs.Add("type", "ping");
                }


                Console.Out.WriteLine("Send Request: {0}", serializer.Serialize(reqargs));
                Program.WriteLine(tcpclient.Client, serializer.Serialize(reqargs));

                Console.Out.WriteLine("Response:\n\n");
                using (StreamReader reader = new StreamReader(tcpclient.GetStream()))
                {
                    int i = 0;
                    while (true)
                    {
                        i++;
                        try
                        {
                            string message = reader.ReadLine();
                            if (message.Contains("###END###"))
                                break;

                            if (debug || (i % 100 == 1))
                                Console.Out.WriteLine("[{0}] {1}", i, message);

                        }
                        catch (Exception)
                        {
                            break;
                        }

                    }
                    Console.Out.WriteLine("Total fetch {0} lines", i);
                }

                Console.Out.WriteLine("\n\n-----------------Request End ---------------------");
            }
        }

        public void HandleRequest() 
        {
            try
            {
                string requesttype = args["type"];

                if (requesttype.Equals("browse")) Browse(tcpclient.Client);
                else if (requesttype.Equals("fetch")) Fetch(tcpclient.Client);
                else Ping();
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine("HandleRequest failed: {0}", e.Message);
                Program.WriteLine(tcpclient.Client, "400, Bad Request, request type argument is not valid" + e.Message);
            }
        }

        private void Ping()
        {

            try{
                string hostname = args["hostname"];
                string servername = args["servername"];

                Console.Out.WriteLine("start to ping ...");
                OPCServer opcserver = new OPCServer();
                opcserver.Connect(servername, hostname);

                if (opcserver.ServerState == (int)OPCServerState.OPCRunning)
                {
                    System.Console.Out.WriteLine("ping {0}:{1} successful", args["hostname"], args["servername"]);
                    Program.WriteLine(tcpclient.Client, "200, true");
                }
                else
                {
                    System.Console.Out.WriteLine("\nping {0}:{1} failed, \n{2}\n", args["hostname"], args["servername"]);
                    Program.WriteLine(tcpclient.Client, "200, false");
                }
            }
            catch
            {
                System.Console.Out.WriteLine("\nping {0}:{1} failed, \n{2}\n", args["hostname"], args["servername"]);
                Program.WriteLine(tcpclient.Client, "200, false");
            }

        }

        private void Browse(Socket writer)
        {
            Console.Out.WriteLine("start to browse ...");
            try
            {
                string hostname = args["hostname"];
                string servername = args["servername"];
                
                OPCServer opcserver = new OPCServer();
                opcserver.Connect(servername, hostname);

                OPCBrowser opcbrowser = opcserver.CreateBrowser();
                //opcbrowser.Filter = args["tagname"];
                //opcbrowser.DataType = short.Parse(args["datatype"]);

                opcbrowser.ShowBranches();
                opcbrowser.ShowLeafs(true);
                long ItemCounts = opcbrowser.Count;
                
                Program.WriteLine(writer, "200, Tags total number: " + ItemCounts);// return the number of browsed tags 

                int count = 0;
                Array PropertyIDs;
                Array Descriptions;
                Array DataTypes;
                Array PropertyValues;
                Array Errors;

                foreach (object tagname in opcbrowser)
                {
                    // tag name, data type, original data type, description, EU type, scan rate, 
                    try
                    {
                        opcserver.QueryAvailableProperties((string)tagname, out count, out PropertyIDs, out Descriptions, out DataTypes);
                        opcserver.GetItemProperties((string)tagname, count, ref PropertyIDs, out PropertyValues, out Errors);

                        if (count <= 0 || count > 100)
                            return;


                        Dictionary<string, object> tag = new Dictionary<string, object>();
                        tag.Add("Name", tagname);
                        tag.Add("DataType", DataTypes.GetValue(2));
                        tag.Add("SourceDataType", DataTypes.GetValue(1));
                        tag.Add("UnitsType", DataTypes.GetValue(7));
                        tag.Add("UnitsInfo", PropertyValues.GetValue(8));
                        tag.Add("ScanRate", PropertyValues.GetValue(6));
                        

                        if (count > 8)
                            tag.Add("Description", (string)PropertyValues.GetValue(9));

                        Program.WriteLine(writer, serializer.Serialize(tag));
                    }
                    catch (Exception ex)
                    {
                        System.Console.Out.WriteLine("Error {0}:{1}\n", tagname, ex.Message);
                    }
                }

                opcserver.Disconnect();
                Program.WriteLine(writer, "###END###");
            }
            catch
            {
                System.Console.Out.WriteLine("\browse {0}:{1} failed, \n{2}\n", args["hostname"], args["servername"]);
                Program.WriteLine(tcpclient.Client, "200, false");
            }          

        }

        private void Fetch(Socket writer)
        {
            Console.Out.WriteLine("start to fetch ...");

            try
            {
                string hostname = args["hostname"];
                string servername = args["servername"];

                OPCServer opcserver = new OPCServer();
                opcserver.Connect(servername, hostname);

                string tagsource = "";
                if (args.ContainsKey("tagsource") && !string.IsNullOrEmpty(args["tagsource"]))
                    tagsource = args["tagsource"];

                List<string> taglist = new List<string>();
                if (tagsource.Equals("browse"))
                {
                    OPCBrowser opcbrowser = opcserver.CreateBrowser();

                    opcbrowser.ShowBranches();
                    opcbrowser.ShowLeafs(true);
                    long ItemCounts = opcbrowser.Count;

                    Program.WriteLine(writer, "200, Tags total number: " + ItemCounts);// return the number of browsed tags 
                    foreach (object tagname in opcbrowser)
                    {
                        WriteLineTag(writer, opcserver, (string)tagname);
                    }
                }
                else if (tagsource.Equals("list"))
                {
                    string list = args["list"]; // must
                    foreach (string tagname in TagsFromList(list))
                    {
                        WriteLineTag(writer, opcserver, (string)tagname);
                    }
                }
                else //if (tagsource.Equals("lines"))
                {
                    while (true)
                    {
                        string line = reader.ReadLine();
                        if (line.Contains("###END###"))
                        {
                            break;
                        }
                        foreach (string tagname in TagsFromList(line))
                        {
                            WriteLineTag(writer, opcserver, (string)tagname);
                        }
                    }
                }

                opcserver.Disconnect();
                Program.WriteLine(writer, "###END###");
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine("\nFetch tag values from {0}:{1} failed, \n{2}\n", args["servername"], args["hostname"], e.Message);
                Program.WriteLine(writer, "200, false, " + e.Message);
            }    
        }

        private static List<string> TagsFromList(string list)
        {
            string[] tagarray = list.Split(',');
            List<string> tags = new List<string>();

            for (int i = 0; i < tagarray.Length; i++)
            {
                tags.Add(tagarray[i]);
            }

            return tags;
        }

        private static void WriteLineTag(Socket writer, OPCServer opcserver,string tagname)
        {
            try
            {
                int count = 0;
                Array PropertyIDs;
                Array Descriptions;
                Array DataTypes;
                Array PropertyValues;
                Array Errors;

                opcserver.QueryAvailableProperties((string)tagname, out count, out PropertyIDs, out Descriptions, out DataTypes);
                opcserver.GetItemProperties((string)tagname, count, ref PropertyIDs, out PropertyValues, out Errors);

                if (count <= 0 || count > 100)
                    return;

                // [TagName, Confidence, Value, Timestamp]
                Dictionary<string, object> tag = new Dictionary<string, object>();
                tag.Add("TagName", tagname);
                tag.Add("Quality", PropertyValues.GetValue(3));
                tag.Add("Value", PropertyValues.GetValue(2));
                tag.Add("TimeStamp", PropertyValues.GetValue(4).ToString());

                Program.WriteLine(writer, serializer.Serialize(tag));
            }
            catch (Exception ex)
            {
                System.Console.Out.WriteLine("Error {0}:{1}\n", tagname, ex.Message);
            }
            
        }
    }
}
