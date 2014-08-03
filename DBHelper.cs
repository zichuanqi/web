using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.OracleClient;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace configtool
{
    public class DBHelper
    {
        public static string GetConnetString(DataTable dt, string name)
        {
            string connectString = "";
            DataRow[] drArray = dt.Select(string.Format("Name='{0}'", name));
            if (drArray.Length > 0)
            {
                DataRow dr = drArray[0];
                string server = dr["Server"].ToString();
                string db = dr["Database"].ToString();
                string username = dr["Username"].ToString();
                string pwd = dr["Password"].ToString();
                connectString = GetOracleConnetString(server, db, username, pwd);
            }
            return connectString;
        }

        public static string GetSQL(string sql, string currentTagName, string currentTagValue, DataTable dt)
        {
            if (string.IsNullOrWhiteSpace(sql)) return "";

            sql = sql.ToLower().Replace("{tagname}", currentTagName).Replace("{tagvalue}", currentTagValue);
            WebAccessAPI api = new WebAccessAPI();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (sql.IndexOf("{no" + (i + 1) + ".name}") > -1)
                {
                    string tagName = dt.Rows[i]["TagName"].ToString();
                    sql = sql.Replace("{no" + (i + 1) + ".name}", tagName);
                    string tagValue = api.GetTagValueByTagName(tagName);
                }
                if (sql.IndexOf("{no" + (i + 1) + ".value}") > -1)
                {
                    string tagName = dt.Rows[i]["TagName"].ToString();
                    string tagValue = api.GetTagValueByTagName(tagName);
                    sql = sql.Replace("{no" + (i + 1) + ".value}", tagValue);
                }
            }

            return sql;
        }

        #region ODBC

        //public static bool TestConnect(string dsn, out string errMsg)
        //{
        //    errMsg = string.Empty;
        //    bool flag = false;
        //    using (OdbcConnection con = new OdbcConnection("DSN=" + dsn))
        //    {
        //        try
        //        {
        //            con.Open();
        //            flag = true;
        //        }
        //        catch (Exception ex)
        //        {
        //            errMsg = ex.Message;
        //        }
        //    }
        //    return flag;
        //}

        //public static bool TestExtcuteSQL(string dsn, string sql, out string errMsg)
        //{
        //    errMsg = string.Empty;
        //    bool flag = false;
        //    try
        //    {
        //        using (OdbcConnection con = new OdbcConnection("DSN=" + dsn))
        //        {
        //            con.Open();
        //            using (OdbcTransaction tran = con.BeginTransaction())
        //            {
        //                using (OdbcCommand cmd = new OdbcCommand(sql, con))
        //                {
        //                    try
        //                    {
        //                        cmd.Transaction = tran;
        //                        cmd.ExecuteNonQuery();
        //                        tran.Rollback();
        //                        flag = true;
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        tran.Rollback();
        //                        throw ex;
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        errMsg = ex.Message;
        //    }
        //    return flag;
        //}

        //public static int ExecuteSQL(string dsn, string sql)
        //{
        //    int ret = 0;
        //    using (OdbcConnection con = new OdbcConnection("DSN=" + dsn))
        //    {
        //        con.Open();
        //        using (OdbcCommand cmd = new OdbcCommand(sql, con))
        //        {
        //            try
        //            {
        //                ret = cmd.ExecuteNonQuery();
        //            }
        //            catch (Exception ex)
        //            {
        //                throw ex;
        //            }
        //        }
        //    }
        //    return ret;
        //}

        //public static int ExecuteTranSQL(string dsn, string sql)
        //{
        //    int ret = 0;
        //    using (OdbcConnection con = new OdbcConnection("DSN=" + dsn))
        //    {
        //        con.Open();
        //        using (OdbcTransaction tran = con.BeginTransaction())
        //        {
        //            using (OdbcCommand cmd = new OdbcCommand(sql, con))
        //            {
        //                try
        //                {
        //                    cmd.Transaction = tran;
        //                    ret = cmd.ExecuteNonQuery();
        //                    tran.Commit();
        //                }
        //                catch (Exception ex)
        //                {
        //                    tran.Rollback();
        //                    throw ex;
        //                }
        //            }
        //        }
        //    }
        //    return ret;
        //}

        public static DataSet Query(string dsn, string sql)
        {
            DataSet ds = new DataSet();
            using (OdbcConnection con = new OdbcConnection("DSN=" + dsn))
            {
                con.Open();
                using (OdbcCommand cmd = new OdbcCommand(sql, con))
                {
                    try
                    {
                        OdbcDataAdapter ada = new OdbcDataAdapter(cmd);
                        ada.Fill(ds);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return ds;
        }

        #endregion

        #region OleDB

        //public static string GetOledbConnetString(string server, string db, string username, string pwd)
        //{
        //    StringBuilder sb = new StringBuilder();
        //    sb.Append("Provider=SQLOLEDB");//
        //    sb.Append(";Data Source=" + server);
        //    sb.Append(";Initial Catalog=" + db);
        //    sb.Append(";User ID=" + username);
        //    sb.Append(";Password=" + pwd);
        //    sb.Append(";Persist Security Info=True;");

        //    return sb.ToString();
        //}

        //public static bool OledbTestConnect(string connectString, out string errMsg)
        //{
        //    errMsg = string.Empty;
        //    bool flag = false;
        //    using (OleDbConnection con = new OleDbConnection(connectString))
        //    {
        //        try
        //        {
        //            con.Open();
        //            flag = true;
        //        }
        //        catch (Exception ex)
        //        {
        //            errMsg = ex.Message;
        //        }
        //    }
        //    return flag;
        //}

        //public static bool OledbTestExtcuteSQL(string connectString, string sql, out string errMsg)
        //{
        //    errMsg = string.Empty;
        //    bool flag = false;
        //    try
        //    {
        //        using (OleDbConnection con = new OleDbConnection(connectString))
        //        {
        //            con.Open();
        //            using (OleDbTransaction tran = con.BeginTransaction())
        //            {
        //                using (OleDbCommand cmd = new OleDbCommand(sql, con))
        //                {
        //                    try
        //                    {
        //                        cmd.Transaction = tran;
        //                        cmd.ExecuteNonQuery();
        //                        tran.Rollback();
        //                        flag = true;
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        tran.Rollback();
        //                        throw ex;
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        errMsg = ex.Message;
        //    }
        //    return flag;
        //}

        //public static int OledbExecuteSQL(string connectString, string sql)
        //{
        //    int ret = 0;
        //    using (OleDbConnection con = new OleDbConnection(connectString))
        //    {
        //        con.Open();
        //        using (OleDbCommand cmd = new OleDbCommand(sql, con))
        //        {
        //            try
        //            {
        //                ret = cmd.ExecuteNonQuery();
        //            }
        //            catch (Exception ex)
        //            {
        //                throw ex;
        //            }
        //        }
        //    }
        //    return ret;
        //}

        //public static int OledbExecuteTranSQL(string connectString, string sql)
        //{
        //    int ret = 0;
        //    using (OleDbConnection con = new OleDbConnection(connectString))
        //    {
        //        con.Open();
        //        using (OleDbTransaction tran = con.BeginTransaction())
        //        {
        //            using (OleDbCommand cmd = new OleDbCommand(sql, con))
        //            {
        //                try
        //                {
        //                    cmd.Transaction = tran;
        //                    ret = cmd.ExecuteNonQuery();
        //                    tran.Commit();
        //                }
        //                catch (Exception ex)
        //                {
        //                    tran.Rollback();
        //                    throw ex;
        //                }
        //            }
        //        }
        //    }
        //    return ret;
        //}

        //public static DataSet OledbQuery(string connectString, string sql)
        //{
        //    DataSet ds = new DataSet();
        //    using (OleDbConnection con = new OleDbConnection(connectString))
        //    {
        //        con.Open();
        //        using (OleDbCommand cmd = new OleDbCommand(sql, con))
        //        {
        //            try
        //            {
        //                OleDbDataAdapter ada = new OleDbDataAdapter(cmd);
        //                ada.Fill(ds);
        //            }
        //            catch (Exception ex)
        //            {
        //                throw ex;
        //            }
        //        }
        //    }
        //    return ds;
        //}

        #endregion

        #region Oracle

        public static string GetOracleConnetString(string server, string db, string username, string pwd)
        {
            string constring = string.Format("Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0}) (PORT=1521)))(CONNECT_DATA=(SERVICE_NAME={1})));Persist Security Info=True;User Id={2}; Password={3}", server, db, username, pwd);
            return constring;
        }

        public static bool OracleTestConnect(string connectString, out string errMsg)
        {
            errMsg = string.Empty;
            bool flag = false;
            using (OracleConnection con = new OracleConnection(connectString))
            {
                try
                {
                    con.Open();
                    flag = true;
                }
                catch (Exception ex)
                {
                    errMsg = ex.Message;
                }
            }
            return flag;
        }

        public static bool OracleTestExtcuteSQL(string connectString, string sql, out string errMsg)
        {
            errMsg = string.Empty;
            bool flag = false;
            try
            {
                using (OracleConnection con = new OracleConnection(connectString))
                {
                    con.Open();
                    using (OracleTransaction tran = con.BeginTransaction())
                    {
                        using (OracleCommand cmd = new OracleCommand(sql, con))
                        {
                            try
                            {
                                cmd.Transaction = tran;
                                cmd.ExecuteNonQuery();
                                tran.Rollback();
                                flag = true;
                            }
                            catch (Exception ex)
                            {
                                tran.Rollback();
                                throw ex;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
            }
            return flag;
        }

        public static int OracleExecuteSQL(string connectString, string sql)
        {
            int ret = 0;
            using (OracleConnection con = new OracleConnection(connectString))
            {
                con.Open();
                using (OracleCommand cmd = new OracleCommand(sql, con))
                {
                    try
                    {
                        ret = cmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return ret;
        }

        public static int OracleExecuteTranSQL(string connectString, string sql)
        {
            int ret = 0;
            using (OracleConnection con = new OracleConnection(connectString))
            {
                con.Open();
                using (OracleTransaction tran = con.BeginTransaction())
                {
                    using (OracleCommand cmd = new OracleCommand(sql, con))
                    {
                        try
                        {
                            cmd.Transaction = tran;
                            ret = cmd.ExecuteNonQuery();
                            tran.Commit();
                        }
                        catch (Exception ex)
                        {
                            tran.Rollback();
                            throw ex;
                        }
                    }
                }
            }
            return ret;
        }

        public static DataSet OracleQuery(string connectString, string sql)
        {
            DataSet ds = new DataSet();
            using (OracleConnection con = new OracleConnection(connectString))
            {
                con.Open();
                using (OracleCommand cmd = new OracleCommand(sql, con))
                {
                    try
                    {
                        OracleDataAdapter ada = new OracleDataAdapter(cmd);
                        ada.Fill(ds);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return ds;
        }

        #endregion
    }
}
