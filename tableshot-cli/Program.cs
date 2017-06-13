using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using tableshot.Commands;
using tableshot.Models;

namespace tableshot
{
    internal class Program
    {
        private static readonly ILogger _logger;

        static Program()
        {
            // Add logging
            if (Debugger.IsAttached)
            {
                ApplicationLogging.LoggerFactory.AddConsole(LogLevel.Debug);
                ApplicationLogging.LoggerFactory.AddDebug(LogLevel.Trace);
            }
            else
                ApplicationLogging.LoggerFactory.AddConsole(LogLevel.Information);

            _logger = ApplicationLogging.CreateLogger<Program>();
        }

        public static ConfigurationModel Configuration { get; private set; }

        private static CommandLineApplication AddCommand<TCommand>(CommandLineApplication application)
            where TCommand : ICommand
        {
            var command = (ICommand) Activator.CreateInstance<TCommand>();

            return application.Command(command.Name, configuration =>
            {
                configuration.Description = command.Description;
                configuration.HelpOption("-h|--help");

                // options for all commands
                var configOption = configuration.Option("--config", "path to config file", CommandOptionType.SingleValue);
                var sourceServerOption = configuration.Option("--source", "override source database (<username>:<pass>@<server>:<database>)", CommandOptionType.SingleValue);
                var targetServerOption = configuration.Option("--target", "override target database (<username>:<pass>@<server>:<database>)", CommandOptionType.SingleValue);
                var tablesOption = configuration.Option("--table", "override tables to clone ([Schema].Table:<none|schema|full>)", CommandOptionType.MultipleValue);

                // specific command configuration
                command.Configure(configuration);

                configuration.OnExecute(async () =>
                {
                    // parse the config json file
                    var path = configOption.HasValue()
                        ? configOption.Value()
                        : "config.json";

                    if (File.Exists(path))
                    {
                        using (var reader = File.OpenText(path))
                        using (var jsonReader = new JsonTextReader(reader))
                        {
                            var serializer = new JsonSerializer();
                            Configuration = serializer.Deserialize<ConfigurationModel>(jsonReader);
                        }
                    }

                    if (sourceServerOption.HasValue())
                        Configuration.SourceCredentials = Util.ParseCredentials(sourceServerOption.Value());
                    if (targetServerOption.HasValue())
                        Configuration.TargetCredentials = Util.ParseCredentials(targetServerOption.Value());
                    if (tablesOption.HasValue())
                        Configuration.Tables = tablesOption.Values.Select(Util.ParseCloneTable).ToArray();

                    // execute the actual command
                    await command.Execute();
                    return 0;
                });
            });
        }

        private static void Main(string[] args)
        {
            MainImpl(args).Wait();
        }

        private static async Task MainImpl(string[] args)
        {
            _logger.LogInformation("application started");

            var commandApplication = new CommandLineApplication(false)
            {
                Name = "tableshot",
                Description = "program to clone selective tables to another database"
            };

            commandApplication.HelpOption("-h|--help");

            // commands
            AddCommand<ExportCommand>(commandApplication);
            AddCommand<ResolveCommand>(commandApplication);
            AddCommand<CloneCommand>(commandApplication);

            commandApplication.Execute(args);

            _logger.LogInformation("application completed");
        }
    }
}
