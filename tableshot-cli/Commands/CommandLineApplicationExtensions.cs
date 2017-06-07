using System;
using Microsoft.Extensions.CommandLineUtils;

namespace tableshot.Commands
{
    internal static class CommandLineApplicationExtensions
    {
        public static CommandLineApplication Command<TCommand>(this CommandLineApplication application)
            where TCommand : ICommand
        {
            var command = (ICommand) Activator.CreateInstance<TCommand>();

            return application.Command(command.Name, configuration =>
            {
                configuration.Description = command.Description;
                configuration.HelpOption("-h|--help");

                command.Configure(configuration);

                configuration.OnExecute(async () =>
                {
                    await command.Execute();
                    return 0;
                });
            });
        }
    }
}
