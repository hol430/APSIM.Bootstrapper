using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using APSIM.Server.Commands;
using CommandLine;

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
                bootstrapper.Run();
                ICommand command = new RunCommand(new Models.Core.Run.IReplacement[0]);
                bootstrapper.SendCommand(command);
                bootstrapper.SendCommand(command);

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
