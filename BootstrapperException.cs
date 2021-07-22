using System;
using Microsoft.Rest;

namespace APSIM.Cluster
{
    /// <summary>
    /// Encapsulates a kubernetes API exception.
    /// </summary>
    public class BootstrapperException : Exception
    {
        /// <summary>
        /// Instantiate a new <see cref="BootstrapperException"/> object.
        /// </summary>
        /// <param name="message">Error message.</param>
        /// <param name="innerException">Inner exception.</param>
        public BootstrapperException(string message, Exception innerException) : base(GetMessage(message, innerException), innerException)
        {
        }

        private static string GetMessage(string message, Exception innerException)
        {
            if (innerException is HttpOperationException httpError)
                return $"{message}\nrequest:\n{httpError.Request.Content}\nresponse:{httpError.Response.Content}";
            else
                return message;
        }
    }
}
