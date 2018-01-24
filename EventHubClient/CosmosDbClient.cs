using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubClientTest
{
    class CosmosDbClient
    {
        static void Main(string[] args)
        {
            int NullCount = 0;
            int mainCount = 0;
            var cosmos = new DocumentClient(new Uri("https://ppetelemetry.documents.azure.com:443/"), "Gb1YIo9kTI3L2JEPFzGMHdjOB8vkAxeyqoh3pa4iyg5mApFXgw9LpqnDvD3MbCJ1WpocZXkntoKpJ2M0G2pYBA==");
            var path = "dbs/ppetelemetry/colls/test";
            //var k = s.ReadPartitionKeyRangeFeedAsync("dbs/ppetelemetry/colls/perftest2/pkranges").Result;

            var data = new { Name = "Praveen Kumar", Age = 30, Department = 10 };
            var collections = new List<TrackEvent>();
            var count = 1000;
            Parallel.For(0, count, new ParallelOptions() { MaxDegreeOfParallelism = 10 }, (a) => { collections.Add(new TrackEvent()); });
            Thread.Sleep(1000);
            Console.WriteLine("Total " + collections.Count);
            var groups = collections;//.GroupBy(item => item.Partition);
            var startTime = DateTime.UtcNow;
            //var d1 = cosmos.CreateDocumentAsync(path, groups[0], new RequestOptions { PartitionKey = new Microsoft.Azure.Documents.PartitionKey(groups[0].Partition) }).Result;
            //var res = Parallel.ForEach(groups, new ParallelOptions() { MaxDegreeOfParallelism = 10,  }, group => {
            //    //var request = JsonConvert.SerializeObject(group, Formatting.None);
            //    //if (group != null)
            //    //    cosmos.CreateDocumentAsync(path, group, new RequestOptions { PartitionKey = new Microsoft.Azure.Documents.PartitionKey(group.Partition) });
            //    //else
            //    if (group != null)
            //    {
            //        cosmos.CreateDocumentAsync(path, group, new RequestOptions { PartitionKey = new Microsoft.Azure.Documents.PartitionKey(group.Partition) });
            //        //Console.WriteLine(group.Custom.First(f => f.Name == "num").Value);
            //        Interlocked.Increment(ref mainCount);
            //    }
            //    else Interlocked.Increment(ref NullCount);
            //});
            //Task.Run(() => { throw new Exception("DD"); });
            //foreach (var item in collections)
            //{
            //    Task.Run(() => {
            //        if (item != null)
            //        {
            //            cosmos.CreateDocumentAsync(path, item, new RequestOptions { PartitionKey = new Microsoft.Azure.Documents.PartitionKey(item.Partition) });
            //            //Console.WriteLine(group.Custom.First(f => f.Name == "num").Value);
            //            Interlocked.Increment(ref mainCount);
            //        }
            //        else Interlocked.Increment(ref NullCount);
            //    });

            //}

            Thread.Sleep(2000);
            //Console.WriteLine("{0};{1};{2}", startTime, DateTime.UtcNow, DateTime.UtcNow.Subtract(startTime).TotalMilliseconds);
            Console.WriteLine("null " + NullCount);
            Console.WriteLine("Main " + mainCount);
            Console.ReadLine();
        }
    }

    public class TrackEvent
    {
        private static int i = 0;
        public TrackEvent()
        {
            var guid = Guid.NewGuid().ToString();
            this.Partition = this.GetPartitionKey(guid);
            var num = Interlocked.Increment(ref i);
            this.Custom = new List<Pair>()
            {
                new Pair() { Name = "CP", Value=guid },
                new Pair() { Name = "num", Value= num.ToString() },
                new Pair() { Name = "abc", Value="fhjfjhg868jgjgjgjgjgjkjh" },
                new Pair() { Name = "def", Value="fhjfjh979gjgjgj8976877kjh" },
                new Pair() { Name = "ijk", Value="fhjfjh979gjgjgjhlu98h8hojojkjh" },
                new Pair() { Name = "lmn", Value="fhjfjhfhfuioh979gjgjgjkjh" },
                new Pair() { Name = "abc", Value="fhjfjhg868jgjgjgjgjgjkjh" },
                new Pair() { Name = "def", Value="fhjfjh979gjgjgj8976877kjh" },
                new Pair() { Name = "ijk", Value="fhjfjh979gjgjgjhlu98h8hojojkjh" },
                new Pair() { Name = "lmn", Value="fhjfjhfhfuioh979gjgjgjkjh" },
                new Pair() { Name = "abc", Value="fhjfjhg868jgjgjgjgjgjkjh" },
                new Pair() { Name = "def", Value="fhjfjh979gjgjgj8976877kjh" },
                new Pair() { Name = "ijk", Value="fhjfjh979gjgjgjhlu98h8hojojkjh" },
                new Pair() { Name = "lmn", Value="fhjfjhfhfuioh979gjgjgjkjh" },
            };
            this.TimeToLive = 100000;
        }

        private string GetPartitionKey(string guid)
        {
            var date = DateTime.UtcNow;
            return string.Format("{0}{1}{2}", date.DayOfYear, date.Year, guid[0]);
        }

        /// <summary>
        /// Gets or sets Name 
        /// </summary>
        public string Partition { get; set; }

        /// <summary>
        /// Gets or sets Name 
        /// </summary>
        public IList<Pair> System { get; set; } = new List<Pair>();

        /// <summary>
        /// Gets or sets Name 
        /// </summary>
        public IList<Pair> Domain { get; set; } = new List<Pair>();

        /// <summary>
        /// Gets or sets Name 
        /// </summary>
        public IList<Pair> Custom { get; set; } = new List<Pair>();

        /// <summary>
        /// Gets or sets TimeToLive
        /// </summary>
        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? TimeToLive { get; set; }
    }

    public class Pair
    {
        /// <summary>
        /// Gets or sets Name 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets Name 
        /// </summary>
        public string Value { get; set; }
    }
}
