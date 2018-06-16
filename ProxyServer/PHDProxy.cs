using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Uniformance.PHD;
using System.Net.Sockets;
using System.IO;
using System.Data;
using System.Web.Script.Serialization;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Net;

namespace PHDProxyServer
{
    class PHDProxy
    {
        static JavaScriptSerializer serializer = new JavaScriptSerializer();
        static int TAGS_MAX_COUNT = 20;
        static int SERVER_PORT = 9001;

        public static void HandleRequest(TcpClient tcpclient, StreamReader reader, Dictionary<string, string> args) 
        {
            string hostname;
            string requesttype;
            try
            {
                hostname = args["hostname"];
                requesttype = args["type"]; // [browse, fetch, ping]: default is ping
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine("hostname argument not available: {0}", e.Message);
                WriteLine(tcpclient.Client, "400, Bad Request, hostname argument not available");
                return;
            }

            using (PHDHistorian oPhd = new PHDHistorian())
            {
                using (PHDServer server = new PHDServer(hostname, SERVERVERSION.RAPI200))
                {

                    if (args.ContainsKey("port") && !string.IsNullOrEmpty(args["port"]))
                        server.Port = int.Parse(args["port"]);
                    //SafeGet(args, "username", server.UserName);
                    if (args.ContainsKey("username") && !string.IsNullOrEmpty(args["username"]))
                        server.UserName = args["username"];
                    if (args.ContainsKey("password") && !string.IsNullOrEmpty(args["password"]))
                        server.Password = args["password"];
                    if (args.ContainsKey("windowsuser") && !string.IsNullOrEmpty(args["windowsuser"]))
                        server.WindowsUsername = args["windowsuser"];
                    if (args.ContainsKey("windowspwd") && !string.IsNullOrEmpty(args["windowspwd"]))
                        server.WindowsPassword = args["windowspwd"];

                    oPhd.DefaultServer = server;

                    if (requesttype.Equals("browse"))
                    {
                        Console.Out.WriteLine("start to browse ...");
                        Browse(args, tcpclient.Client, oPhd);
                    }
                    else if (requesttype.Equals("fetch"))
                    {
                        Console.Out.WriteLine("start to fetch ...");
                        Fetch(args, reader, tcpclient.Client, oPhd);
                    }
                    else
                    {
                        Console.Out.WriteLine("start to ping ...");
                        Ping(args, tcpclient.Client, oPhd);
                    }

                }
            }
        }

        public static void SendRequest(string[] args)
        {
            using (TcpClient tcpclient = new TcpClient())
            {
                IPAddress address = IPAddress.Parse("127.0.0.1");
                IPEndPoint point = new IPEndPoint(address, SERVER_PORT);
                //tcpclient.ReceiveTimeout = 5000;

                tcpclient.Connect(point);

                StreamReader reader = new StreamReader(tcpclient.GetStream());

                string hostname = Program.GetArgument(args, "hostname");
                bool ping = Program.HasOption(args, "ping");
                bool browse = Program.HasOption(args, "browse");
                bool fetch = Program.HasOption(args, "fetch");
                bool debug = Program.HasOption(args, "debug");

                if (fetch)
                {
                    //phd.PrintQuery(tag.Split(','), Program.GetArgument(args, "start", "now-1h"), Program.GetArgument(args, "end", "now"));
                    //phd.ComplexPrintQuery(tag.Split(','), Program.GetArgument(args, "start", "now-1h"), Program.GetArgument(args, "end", "now"), stype, sfreq, rtype, rfreq, offset);
                    string stype = Program.GetArgument(args, "stype", null);
                    string sfreq = Program.GetArgument(args, "sfreq", null);
                    string rtype = Program.GetArgument(args, "rtype", null);
                    string rfreq = Program.GetArgument(args, "rfreq", null);
                    string offset = Program.GetArgument(args, "offset", null);
                    string start = Program.GetArgument(args, "start", "now-1h");
                    string end = Program.GetArgument(args, "end", "now");
                    string tagsource = Program.GetArgument(args, "tagsource", "list"); // [browse, list, lines]
                    string list = Program.GetArgument(args, "list", null);
                    string tagname = Program.GetArgument(args, "tagname", "*");
                    string datatype = Program.GetArgument(args, "datatype", null);
                    string count = Program.GetArgument(args, "count", "100");

                    Dictionary<string, string> fargs = new Dictionary<string, string>();
                    fargs.Add("type", "fetch");
                    fargs.Add("hostname", hostname);
                    fargs.Add("start", start);
                    fargs.Add("end", end);
                    fargs.Add("tagsource", tagsource);
                    fargs.Add("offset", offset);
                    fargs.Add("sampletype", stype);
                    fargs.Add("samplefrequency", sfreq);
                    fargs.Add("reductiontype", rtype);
                    fargs.Add("reductionoffset", offset);
                    fargs.Add("reductionfrequency", rfreq);
                    fargs.Add("list", list);
                    fargs.Add("tagname", tagname);
                    fargs.Add("datatype", datatype);
                    fargs.Add("count", count);

                    Console.Out.WriteLine("Send Request: {0}", serializer.Serialize(fargs));
                    WriteLine(tcpclient.Client, serializer.Serialize(fargs));
                }
                else if (browse)
                {
                    string tagmodel = Program.GetArgument(args, "tagmodel", "onlyname");
                    string tagname = Program.GetArgument(args, "tagname", "*");
                    string datatype = Program.GetArgument(args, "datatype", null);
                    string count = Program.GetArgument(args, "count", "100");
                    Dictionary<string, string> bargs = new Dictionary<string, string>();
                    bargs.Add("type", "browse");
                    bargs.Add("hostname", hostname);
                    bargs.Add("tagmodel", tagmodel);
                    bargs.Add("tagname", tagname);
                    bargs.Add("count", count);
                    Console.Out.WriteLine("Send Request: {0}", serializer.Serialize(bargs));
                    WriteLine(tcpclient.Client, serializer.Serialize(bargs));
                }
                else
                {
                    Dictionary<string, string> pargs = new Dictionary<string, string>();

                    pargs.Add("type", "ping");
                    pargs.Add("hostname", hostname);

                    Console.Out.WriteLine("Send Request: {0}", serializer.Serialize(pargs));
                    WriteLine(tcpclient.Client, serializer.Serialize(pargs));
                }

                Console.Out.WriteLine("Response:\n\n");
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

                Console.Out.WriteLine("\n\n-----------------Request End ---------------------");
            }
        }

        private static void Ping(Dictionary<string, string> args, Socket writer, PHDHistorian oPhd)
        {
            try
            {
                DataSet ds = oPhd.GetLinks();
                System.Console.Out.WriteLine("ping {0}:{1} successful", oPhd.DefaultServer.HostName, oPhd.DefaultServer.Port);
                WriteLine(writer, "200, true");
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine("\nping {0}:{1} failed, \n{2}\n", oPhd.DefaultServer.HostName, oPhd.DefaultServer.Port, e.Message);
                WriteLine(writer, "200, false");
            }
        }

        private static void Browse(Dictionary<string, string> args, Socket writer, PHDHistorian oPhd)
        {
            try
            {
                TagFilter filter = new TagFilter();
                filter.Tagname = "*";

                string tagmodel = "onlyname"; // tag info model, [onlyname, verbose]default is onlyname, 
                uint count = 0; // maximum number of tags to be returned

                if (args.ContainsKey("tagname") && !string.IsNullOrEmpty(args["tagname"]))
                    filter.Tagname = args["tagname"];
                if (args.ContainsKey("datatype") && !string.IsNullOrEmpty(args["datatype"]))
                    filter.DataType = args["datatype"].ToCharArray()[0];
                if (args.ContainsKey("tagmodel") && !string.IsNullOrEmpty(args["tagmodel"]))
                    tagmodel = args["tagmodel"];
                if (args.ContainsKey("count") && !string.IsNullOrEmpty(args["count"]))
                    count = uint.Parse(args["count"]);

                DataSet ds = oPhd.BrowsingTags(count, filter);
                DataTable table = ds.Tables[0];
                WriteLine(writer, "200, Tags total number: " + table.Rows.Count);// return the number of browsed tags 

                foreach (DataRow row in table.Rows)
                {
                    if (tagmodel.Equals("verbose"))
                    {
                        // [Name, Tagno, Description, Units, SourceCollector, DataType, DataSize, SourceTagname, SourceUnits, ParentTagname, ParentTagno]
                        WriteLine(writer, serializer.Serialize(TagModel(row)));
                    }
                    else
                    {
                        WriteLine(writer, (string)row["Name"]);
                    }

                }
                WriteLine(writer, "###END###");
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine("\nBrowse tags from {0}:{1} failed, \n{2}\n", oPhd.DefaultServer.HostName, oPhd.DefaultServer.Port, e.Message);
                WriteLine(writer, "200, false, " + e.Message);
            }

        }

        private static void Fetch(Dictionary<string, string> args, StreamReader reader, Socket writer, PHDHistorian oPhd)
        {
            oPhd.UTCDateTime = true;

            if (args.ContainsKey("starttime") && !string.IsNullOrEmpty(args["starttime"]))
                oPhd.StartTime = args["starttime"];
            if (args.ContainsKey("endtime") && !string.IsNullOrEmpty(args["endtime"]))
                oPhd.EndTime = args["endtime"];
            if (args.ContainsKey("sampletype") && !string.IsNullOrEmpty(args["sampletype"]))
                oPhd.Sampletype = SampleType(args["sampletype"]); // [average, snapshot, resampled, interpolatedraw, raw ]:  default is raw
            if (args.ContainsKey("reductiontype") && !string.IsNullOrEmpty(args["reductiontype"]))
                oPhd.ReductionType = ReductionType(args["reductiontype"]);// [ average, max, min, first, last, none ]: default is none
            if (args.ContainsKey("offset") && !string.IsNullOrEmpty(args["offset"]))
                oPhd.ReductionOffset = ReductionOffset(args["offset"]); // [ after, around, before ]: default is before
            if (args.ContainsKey("samplefrequency") && !string.IsNullOrEmpty(args["samplefrequency"]))
                oPhd.SampleFrequency = uint.Parse(args["samplefrequency"]);
            if (args.ContainsKey("reductionfrequency") && !string.IsNullOrEmpty(args["reductionfrequency"]))
                oPhd.ReductionFrequency = uint.Parse(args["reductionfrequency"]);

            try
            {
                string tagsource = "";
                if (args.ContainsKey("tagsource") && !string.IsNullOrEmpty(args["tagsource"]))
                    tagsource = args["tagsource"];

                List<Tags> taglist = new List<Tags>();
                if (tagsource.Equals("browse"))
                {
                    taglist = TagsFromBrowse(oPhd, args);
                }
                else if (tagsource.Equals("list"))
                {
                    string list = args["list"]; // must
                    taglist.Add(TagsFromList(list));
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
                        taglist.Add(TagsFromList(line));
                    }
                }

                WriteTagValues(oPhd, taglist, writer);
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine("\nFetch tag values from {0}:{1} failed, \n{2}\n", oPhd.DefaultServer.HostName, oPhd.DefaultServer.Port, e.Message);
                WriteLine(writer, "200, false, " + e.Message);
            }
        }

        private static void WriteTagValues(PHDHistorian oPhd, List<Tags> taglist, Socket writer)
        {
            using (BlockingCollection<string> queue = new BlockingCollection<string>())
            {
                Task consumer = new Task(() =>
                {
                    foreach (string value in queue.GetConsumingEnumerable()) // will block waiting for completion adding 
                    {
                        WriteLine(writer, value);
                    }

                });

                Task producer = new Task(() =>
                {
                    taglist.ForEach((tags) =>
                    {
                        System.Collections.ArrayList datas = oPhd.FetchStructData(tags);
                        foreach (PHDataStruct pd in datas)
                        {
                            queue.Add(serializer.Serialize(pd)); // [TagName, Confidence, Value, Timestamp, Units]
                        }
                    });

                    queue.CompleteAdding();
                });

                producer.Start();
                consumer.Start();

                producer.Wait();
                consumer.Wait();
            }

        }

        private static SAMPLETYPE SampleType(string stype)
        {
            SAMPLETYPE result = SAMPLETYPE.Raw;
            if (stype != null)
            {
                if (stype.Equals("snapshot"))
                    result = SAMPLETYPE.Snapshot;
                else if (stype.Equals("average"))
                    result = SAMPLETYPE.Average;
                else if (stype.Equals("resampled"))
                    result = SAMPLETYPE.Resampled;
                else if (stype.Equals("interpolatedraw"))
                    result = SAMPLETYPE.InterpolatedRaw;
                else
                    result = SAMPLETYPE.Raw;
            }

            return result;
        }

        private static REDUCTIONTYPE ReductionType(string rtype)
        {
            REDUCTIONTYPE result = REDUCTIONTYPE.Average;

            if (rtype != null)
            {
                if (rtype.Equals("average"))
                    result = REDUCTIONTYPE.Average;
                else if (rtype.Equals("max"))
                    result = REDUCTIONTYPE.Maximum;
                else if (rtype.Equals("min"))
                    result = REDUCTIONTYPE.Minimum;
                else if (rtype.Equals("first"))
                    result = REDUCTIONTYPE.First;
                else if (rtype.Equals("last"))
                    result = REDUCTIONTYPE.Last;
                else
                    result = REDUCTIONTYPE.None;
            }

            return result;
        }

        private static REDUCTIONOFFSET ReductionOffset(string offset)
        {
            REDUCTIONOFFSET result = REDUCTIONOFFSET.Around;
            if (offset != null)
            {
                if (offset.Equals("after"))
                    result = REDUCTIONOFFSET.After;
                else if (offset.Equals("around"))
                    result = REDUCTIONOFFSET.Around;
                else
                    result = REDUCTIONOFFSET.Before;
            }

            return result;
        }

        private static List<Tags> TagsFromBrowse(PHDHistorian oPhd, Dictionary<string, string> args)
        {
            TagFilter filter = new TagFilter();
            filter.Tagname = "*";
            uint count = 0;

            if (args.ContainsKey("tagname") && !string.IsNullOrEmpty(args["tagname"]))
                filter.Tagname = args["tagname"];// tag name pattern, such as "AC-*", default is "*"
            if (args.ContainsKey("datatype") && !string.IsNullOrEmpty(args["datatype"]))
                filter.DataType = args["datatype"].ToCharArray()[0];// data type, such as 'F', 'C', ...
            if (args.ContainsKey("count") && !string.IsNullOrEmpty(args["count"]))
                count = uint.Parse(args["count"]);// maxinum number to browse


            List<Tags> taglist = new List<Tags>();
            DataSet ds = oPhd.BrowsingTags(count, filter);
            DataTable table = ds.Tables[0];
            Tags _tags = new Tags();
            foreach (DataRow row in table.Rows)
            {
                _tags.Add(new Tag((string)row["Name"]));
                if (_tags.Count > TAGS_MAX_COUNT)
                {
                    taglist.Add(_tags);
                    _tags = new Tags();
                }
            }

            if (_tags.Count > 0)
            {
                taglist.Add(_tags);
            }

            return taglist;
        }

        private static Tags TagsFromList(string list)
        {
            string[] tagarray = list.Split(',');
            Tags tags = new Tags();

            for (int i = 0; i < tagarray.Length; i++)
            {
                tags.Add(new Tag(tagarray[i]));
            }

            return tags;
        }

        static void WriteLine(Socket socket, string message)
        {
            byte[] buffer = System.Text.Encoding.ASCII.GetBytes(message + "\n");
            socket.Send(buffer);
        }

        static Dictionary<string, Object> TagModel(DataRow row)
        {
            Dictionary<string, Object> tag = new Dictionary<string, Object>();
            tag.Add("Name", (string)row["Name"]);
            tag.Add("Tagno", (int)row["Tagno"]);
            tag.Add("Description", (string)row["Description"]);
            tag.Add("Units", (string)row["Units"]);
            tag.Add("SourceCollector", (string)row["SourceCollector"]);
            tag.Add("DataType", (char)row["DataType"]);
            tag.Add("DataSize", (int)row["DataSize"]);
            tag.Add("SourceTagname", (string)row["SourceTagname"]);
            tag.Add("SourceUnits", (string)row["SourceUnits"]);

            return tag;
        }
    }
}
