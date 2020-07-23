using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data.Common;

namespace Mysql
{
    public sealed class SqlConnection : IDisposable
    {
        public MySqlConnection Connection;
        public SqlConnection(string cs)
        {
            Connection = new MySqlConnection(cs);
        }

        public async void Dispose()
        {
            await Connection.CloseAsync();
        }
    }

    public class SQLConnector 
    {
        private readonly string Connection;
        public SQLConnector(string connectionString)
        {
            Connection = connectionString;
        }

        public async Task<List<Dictionary<string, object>>> Read(MySqlCommand Command)
        {
            try
            {
                using SqlConnection db = new SqlConnection(Connection);
                await db.Connection.OpenAsync();
                Command.Connection = db.Connection;
                using DbDataReader reader = await Command.ExecuteReaderAsync();
                List<Dictionary<string, object>> results = new List<Dictionary<string, object>>();
                while (await reader.ReadAsync())
                {
                    Dictionary<string, object> row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row.Add(reader.GetName(i), reader.GetValue(i));
                    }
                    results.Add(row);
                }
                return results;
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
        }

        public async Task<List<Dictionary<string, string>>> Read(string query, Dictionary<string, object> entities)
        {
            try
            {
                using (var db = new SqlConnection(Connection))
                {
                    await db.Connection.OpenAsync();
                    using (var cmd = db.Connection.CreateCommand())
                    {
                        cmd.Connection = db.Connection;
                        cmd.CommandText = query;
                        foreach (KeyValuePair<string, object> entity in entities)
                        {
                            cmd.Parameters.AddWithValue(entity.Key, entity.Value);
                        }
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            List<Dictionary<string, string>> data = new List<Dictionary<string, string>>();
                            while (await reader.ReadAsync())
                            {
                                Dictionary<string, string> row = new Dictionary<string, string>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row.Add(reader.GetName(i), reader.GetValue(i).ToString());
                                }
                                data.Add(row);
                            }
                            return data;
                        }
                    }
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
        }
        public async Task<List<Dictionary<string, object>>> Read(string query)
        {
            try
            {
                using (var db = new SqlConnection(Connection))
                {
                    await db.Connection.OpenAsync();
                    using (var cmd = db.Connection.CreateCommand())
                    {
                        cmd.Connection = db.Connection;
                        cmd.CommandText = query;
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            List<Dictionary<string, object>> data = new List<Dictionary<string, object>>();
                            while (await reader.ReadAsync())
                            {
                                Dictionary<string, object> row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row.Add(reader.GetName(i), reader.GetValue(i));
                                }
                                data.Add(row);
                            }
                            return data;
                        }
                    }
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
        }
        public async Task<int> Write(MySqlCommand Command)
        {
            try
            {
                using (SqlConnection db = new SqlConnection(Connection))
                {
                    await db.Connection.OpenAsync();
                    Command.Connection = db.Connection;
                    return await Command.ExecuteNonQueryAsync();
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
        }
        public async Task<int> Write(string query, Dictionary<string, object> list)
        {
            try
            {
                using (var db = new SqlConnection(Connection))
                {

                    await db.Connection.OpenAsync();
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = db.Connection;
                        cmd.CommandText = query;
                        foreach (KeyValuePair<string, object> value in list)
                        {
                            cmd.Parameters.AddWithValue(value.Key, value.Value);
                        }
                        return await cmd.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
        }
        public async Task<int> Write(string query)
        {
            try
            {
                using (var db = new SqlConnection(Connection))
                {
                    await db.Connection.OpenAsync();
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = db.Connection;
                        cmd.CommandText = query;
                        return await cmd.ExecuteNonQueryAsync();
                    }

                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
        }
    }
}
