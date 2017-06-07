using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;

namespace tableshot.Commands
{
    internal interface ICommand
    {
        string Name { get; }
        string Description { get; }
        void Configure(CommandLineApplication application);
        Task Execute();
    }
}
