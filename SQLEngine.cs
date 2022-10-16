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

        /*
        public List<Error> Insert()
        {
            List<Error> errors = new List<Error>();

            try
            {

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
         * 
         * 
         * 
         * 
         */

    }
}
