using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Week6Databases.Constants;

namespace Week6Databases
{
    internal class Parser
    {
        List<MyFile> filesToProcess = new List<MyFile>();
        List<Error> errors = new List<Error>();

        bool hasErrors => errors.Any();

        /// <summary>
        /// Parser Constructor takes a list of strings, converts them into MyFile objects, and adds them to the filesToProcess list 
        /// After collecting files and checking for initial errors, parser calls sql engine RunEngineTasks
        /// </summary>
        /// <param name="fileNames">string names of the files that the user wants to parse</param>
        public Parser()
        {
            Console.WriteLine("Process Started!");

            List<string> fileNames = GetAllFiles();

            foreach (var name in fileNames)
            {
                filesToProcess.Add(CreateFile(name));
            }

            if (hasErrors)
            {
                Console.WriteLine("Process exited with errors.");
                foreach (var error in errors)
                {
                    Console.WriteLine($"Error: {error.ErrorMessage} Source: {error.Source}");
                }
                return;
            }

            foreach (var file in filesToProcess)
            {
                SQLEngine engine = new SQLEngine(databaseName, file);
                errors.AddRange(engine.RunSqlTasks());
            }

            if (!hasErrors)
            {
                Console.WriteLine("Process completed succesfully for all items!");
            }
            else
            {
                Console.WriteLine("Process exited with errors!");
                foreach (var error in errors)
                {
                    Console.WriteLine($"Error: {error.ErrorMessage} Source: {error.Source}");
                }
            }
        }

        List<string> GetAllFiles()
        {
            return Directory.GetFiles(directoryPath).Where(x => !x.EndsWith("_out.txt")).ToList();
        }

        /// <summary>
        /// CreateFiles handles the creation of valid MyFile objects, adds an error if one is found
        /// </summary>
        /// <param name="fileName">string name for the file</param>
        MyFile CreateFile(string fileName)
        {
            MyFile file = new MyFile();
            if (fileName.EndsWith(".txt"))
            {
                file.Delimiter = "|";
                file.Extension = ".txt";
            }
            else
            {
                errors.Add(new Error($"Invalid File Extension, {fileName.Substring(fileName.LastIndexOf("."))} is not supported", $"{fileName}"));
            }
            file.FilePath = Path.Combine(directoryPath, fileName);

            return file;
        }
    }
}
