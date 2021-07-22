using System;
using System.Collections.Generic;
using System.Text;
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
            try
            {
                Bootstrapper bootstrapper = new Bootstrapper(options);
                bootstrapper.Run();
            }
            catch (Exception err)
            {
                Console.Error.WriteLine(err);
                exitCode = 1;
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
