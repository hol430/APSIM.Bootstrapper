using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using APSIM.Server.Commands;
using APSIM.Shared.Utilities;
using CommandLine;
using Models.Core.Run;

namespace APSIM.Cluster
{
    class Program
    {
        private static int exitCode = 0;

        static int Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            new Parser(config =>
            {
                config.AutoHelp = true;
                config.HelpWriter = Console.Out;
            }).ParseArguments<Options>(args)
              .WithParsed(Run)
              .WithNotParsed(HandleParseError);
            return exitCode;
        }

        /// <summary>
        /// Run the job manager.
        /// </summary>
        /// <param name="options">Options</param>
        private static void Run(Options options)
        {
            Bootstrapper bootstrapper = null;
            try
            {
                bootstrapper = new Bootstrapper(options);

                // 1. Initialise the job.
                bootstrapper.Initialise();

                // Let's do this bit twice, just for fun.
                for (int i = 0; i < 2; i++)
                {
                    // 2. Run everything.
                    RunCommand command = new RunCommand(new IReplacement[0]);
                    bootstrapper.RunWithChanges(command);

                    // 3. Read outputs.
                    IEnumerable<string> parameters = new[]
                    {
                        "Date",
                        "BiomassWt",
                        "Yield"
                    };
                    ReadCommand readCommand = new ReadCommand("Report", parameters);
                    DataTable outputs = bootstrapper.ReadOutput(readCommand);
                    Console.WriteLine("Received output from cluster:");
                    Console.WriteLine(DataTableUtilities.ToMarkdown(outputs, true));
                }

                // 4. Cleanup all provisioned resources.
                bootstrapper.Dispose();

                // next - test rerunning with changed inputs - should cause changed outputs
                // bootstrapper.RunWithChanges(command);

                // TestCopyFile.TestCompressToProcess();

                // MoreTests.Run();
            }
            catch (Exception err)
            {
                Console.Error.WriteLine(err);
                exitCode = 1;
                if (bootstrapper != null)
                    bootstrapper.Dispose();
            }
        }

        /// <summary>
        /// Handles parser errors to ensure that a non-zero exit code
        /// is returned when parse errors are encountered.
        /// </summary>
        /// <param name="errors">Parse errors.</param>
        private static void HandleParseError(IEnumerable<Error> errors)
        {
            if ( !(errors.IsHelp() || errors.IsVersion()) )
                exitCode = 1;
        }
    }
}
