using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
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
            {
                ApplicationLogging.LoggerFactory.AddConsole(LogLevel.Information);
            }

            _logger = ApplicationLogging.CreateLogger<Program>();
        }

        public static IConfigurationRoot Configuration { get; }
        
        private static void Main(string[] args)
        {
            MainImpl(args);
        }

        private static Task MainImpl(string[] args)
        {
            _logger.LogDebug("application started");

            var commandApplication = new CommandLineApplication(false)
            {
                Name = "tableshot"
            };

            commandApplication.HelpOption("-h|--help");

            // commands
            commandApplication.Command<ExportCommand>();
            commandApplication.Command<ResolveCommand>();
            commandApplication.Command<CloneCommand>();

            commandApplication.Execute(args);

            _logger.LogDebug("application completed");

            return Task.CompletedTask;
        }
    }
}
