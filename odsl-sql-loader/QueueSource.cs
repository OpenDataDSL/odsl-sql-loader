using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

namespace odsl_sql_loader
{
    public class QueueSource
    {
        private IConfiguration Configuration;
        private SQLDestination SQL;
        public QueueSource(IConfiguration Configuration)
        {
            this.Configuration = Configuration;
            SQL = new SQLDestination(Configuration);
        }
        public async Task ReceiveMessagesAsync()
        {
            await using (ServiceBusClient client = new ServiceBusClient(Configuration["connectionStrings:serviceBus"]))
            {
                // create a processor that we can use to process the messages
                ServiceBusProcessor processor = client.CreateProcessor(Configuration["queueName"], new ServiceBusProcessorOptions());

                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Press any key to end the processing");
                Console.ReadKey();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
            }
        }

        // handle received messages
        async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            try
            {
                SQL.WriteMessage(body);

                // complete the message, message is deleted from the queue. 
                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception e)
            {
                // Message failed to load, so move it to the dead letter queue
                await args.DeadLetterMessageAsync(args.Message, e.Message);
            }
        }

        // handle any errors when receiving messages
        Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
