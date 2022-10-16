using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.Reflection.PortableExecutable;

namespace Week6Databases
{

    internal class SQLEngine
    {
        string SqlConString { get; set; }
        string TableName { get; set; }

        public SQLEngine(string tableName)
        {
            TableName = tableName;

            // sourced from powerpoint example
            SqlConnectionStringBuilder sqlConStringBuilder = new SqlConnectionStringBuilder();
            sqlConStringBuilder["server"] = @"(localdb)\MSSQLLocalDB";
            sqlConStringBuilder["Trusted_Connection"] = true;
            sqlConStringBuilder["Integrated Security"] = "SSPI";
            sqlConStringBuilder["Initial Catalog"] = "PROG260FA22";

            SqlConString = sqlConStringBuilder.ToString();
        }

        /// <summary>
        /// Insert reads all data from the specified file and adds the data to the table specified by tablename
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public List<Error> Insert(MyFile file)
        {
            List<Error> errors = new List<Error>();
            List<string[]> lines = new List<string[]>();
            string header = "";

            try
            {
                using (StreamReader sr = new StreamReader(file.FilePath))
                {
                    int index = 0;
                    while (!sr.EndOfStream)
                    {
                        if (index == 0)
                        {
                            var headerItems = sr.ReadLine()?.Split(",") ?? new string[0];
                            header = SetHeaders(headerItems);
                        }
                        else
                        {
                            var lineItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            lines.Add(lineItems);
                        }
                        
                        index++;
                    }
                }

                // Powerpoint example
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    foreach (var item in lines)
                    {
                        string inLineSql = $@"INSERT INTO {TableName} {header} VALUES ('{item[0]}', '{item[1]}', '{SqlMoney.Parse(item[2])}', '{item[3]}', '{item[4]}')";

                        using (var command = new SqlCommand(inLineSql, conn))
                        {
                            var query = command.ExecuteNonQuery();
                        }
                    }

                    conn.Close();
                }
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }

            return errors;
        }

        /// <summary>
        /// SetHeaders can be used on the string array pulled from the first line of the DB to set the correct header structure for inline sql
        /// </summary>
        /// <param name="_headers"></param>
        /// <returns></returns>
        public string SetHeaders(string[] _headers)
        {
            string temp = "";
            for (int i = 0; i < _headers.Length; i++)
            {
                if (i == 0)
                {
                    temp += $"[{_headers[i]}]";
                }
                else
                {
                    temp += $", [{_headers[i]}]";
                }

            }
            return $"({temp})";
        }

        /// <summary>
        /// UpdateLocations will update any locations that include F with Z
        /// </summary>
        /// <returns></returns>
        public List<Error> UpdateLocations()
        {
            List<Error> errors = new List<Error>();

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();
                    // Example - https://www.w3schools.com/sql/func_sqlserver_replace.asp
                    string inLineSql = $@"UPDATE {TableName} SET Location = REPLACE(Location, 'F', 'Z')";

                    using (var command = new SqlCommand(inLineSql, conn))
                    {
                        var query = command.ExecuteNonQuery();
                    }

                    conn.Close();
                }
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }

        /// <summary>
        /// DeleteExpiredEntries deletes any entries where the current date is greater than the sell by date
        /// </summary>
        /// <returns></returns>
        public List<Error> DeleteExpiredEntries()
        {
            List<Error> errors = new List<Error>();

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    // GETDATE() example found at - https://www.w3schools.com/sql/func_sqlserver_getdate.asp
                    string inLineSql = $@"DELETE FROM {TableName} WHERE GETDATE() > Sell_by_Date";

                    using (var command = new SqlCommand(inLineSql, conn))
                    {
                        var query = command.ExecuteNonQuery();
                    }

                    conn.Close();
                }
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }

            return errors;
        }

        /// <summary>
        /// Increments all prices by 1
        /// </summary>
        /// <returns></returns>
        public List<Error> IncrementPrice()
        {
            List<Error> errors = new List<Error>();

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string inLineSql = $@"UPDATE {TableName}" +
                                       $"SET Price = Price + 1";

                    using (var command = new SqlCommand(inLineSql, conn))
                    {
                        var query = command.ExecuteNonQuery();
                    }

                    conn.Close();
                }
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }

        /// <summary>
        /// Reads all data collected by the inline sql command, adds values to a dictionary and srites them to a new text file
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public List<Error> ExportData(MyFile file)
        {
            List<Error> errors = new List<Error>();
            int fields = 6;

            try
            {
                Dictionary<int, List<string>> lines = new Dictionary<int, List<string>>();
                string writePath = file.FilePath.Replace(file.Extension, $"_out.txt");

                if (File.Exists(writePath))
                {
                    File.Delete(writePath);
                }


                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string inLineSql = $@"SELECT * FROM {TableName}";

                    using (var command = new SqlCommand(inLineSql, conn))
                    {
                        var reader = command.ExecuteReader();
                        int index = 0;
                        while (reader.Read())
                        {
                            List<string> temp = new List<string>();
                            for (int i = 0; i < fields; i++)
                            {
                                temp.Add($"{reader.GetValue(i)}");
                            }
                            lines.Add(index, temp);
                            index++;
                        }
                    }

                    conn.Close();
                }

                using (StreamWriter sw = new StreamWriter(writePath, true))
                {
                    sw.WriteLine($"Processed at: {DateTime.Now}");
                    sw.WriteLine();
                    sw.WriteLine("ID,Name,Location,Price,UoM,Sell_by_Date");

                    foreach (var item in lines)
                    {
                        string temp = "";
                        for (int i = 0; i < item.Value.Count; i++)
                        {
                            if (i == item.Value.Count - 1)
                            {
                                temp += item.Value[i];
                            }
                            else
                            {
                                temp += $"{item.Value[i]}|";
                            }
                        }
                        sw.WriteLine(temp);
                    }
                }
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }

    }
}
