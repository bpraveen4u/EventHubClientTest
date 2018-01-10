using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubClientTest
{

    class Message
    {
        static int counter;
        public Message()
        {
            Interlocked.Increment(ref counter);

            this.Id = counter;
            this.MyGuid = Guid.NewGuid().ToString();
        }
        public int Id { get; set; }
        public string MyGuid { get; set; }

        public override string ToString()
        {
            return this.Id.ToString() + ":" + this.MyGuid;
        }
    }
    class Program
    {
        static string eventHubName = "counter";
        static string connectionString = "Endpoint=sb://praveeneh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1LPMHtefJnAwYVcEoB+BYyMfjVupmywf33nlAjUhfwc=";

        static void Main(string[] args)
        {
            Console.WriteLine("Press Ctrl-C to stop the sender process");
            Console.WriteLine("Press Enter to start now");
            Console.ReadLine();
            SendingRandomMessages();
            Console.ReadLine();
        }



        static void SendingRandomMessages()
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
            //while (true)
            //{
            try
            {
                Console.WriteLine("START " + DateTime.Now);
                var result = Parallel.For(1, 1001, (a) =>
                {
                    var message = new Message();
                    var stringMessage = JsonConvert.SerializeObject(message);
                        //Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, stringMessage);
                        eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(stringMessage)));
                });

            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                Console.ResetColor();
            }
            Console.WriteLine("end " + DateTime.Now);
            Thread.Sleep(1000);
            Console.WriteLine();
            //}
        }
    }
}
