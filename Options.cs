using CommandLine;

namespace APSIM.Cluster
{
    public class Options
    {
        /// <summary>
        /// Input file.
        /// </summary>
        [Option('f', "file", HelpText = "Input file")]
        public string InputFile { get; set; }

        /// <summary>
        /// Number of vCPUs per worker node.
        /// </summary>
        /// <value></value>
        [Option('c', "cpu-count", HelpText = "Number of vCPUs per worker node", Default = 1u)]
        public uint CpuCount { get; set; }

        /// <summary>Show verbose outputs</summary>
        [Option('v', "verbose", HelpText = "Show verbose outputs")]
        public bool Verbose { get; set; }
    }
}