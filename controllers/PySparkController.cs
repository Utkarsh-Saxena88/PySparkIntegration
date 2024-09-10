using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace PySparkIntegration.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PySparkController : ControllerBase
    {
        [HttpGet("run-pyspark")]
        public IActionResult RunPySparkJob()
        {
            try
            {
                var pythonPath = "python";  // Path to Python interpreter
                var scriptPath = Path.Combine(Directory.GetCurrentDirectory(), "Scripts", "pyspark_job.py");

                var processStartInfo = new ProcessStartInfo
                {
                    FileName = pythonPath,
                    Arguments = scriptPath,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (var process = new Process())
                {
                    process.StartInfo = processStartInfo;
                    process.Start();

                    string result = process.StandardOutput.ReadToEnd();
                    string error = process.StandardError.ReadToEnd();

                    process.WaitForExit();

                    if (!string.IsNullOrEmpty(error))
                    {
                        return StatusCode(500, $"Error: {error}");
                    }

                    return Ok($"PySpark Job Output: {result}");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }
    }
}
