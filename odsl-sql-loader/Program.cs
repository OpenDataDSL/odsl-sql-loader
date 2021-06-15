using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;

namespace odsl_sql_loader
{
    class Program
    {
        private IConfiguration Configuration;

        static async Task Main()
        {
            Program p = new Program();
            p.Init();
            await p.StartListening();
        }
        void Init()
        {
            // Build configuration
            Configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetParent(AppContext.BaseDirectory).FullName)
                .AddJsonFile("appsettings.json", false)
                .Build();
        }

        async Task StartListening()
        {
            QueueSource queueSource = new QueueSource(Configuration);
            await queueSource.ReceiveMessagesAsync();
        }
    }
}
