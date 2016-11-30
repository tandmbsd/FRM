using HybridService.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;
using System.Messaging;
using Microsoft.Crm.Sdk;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using System.Threading;
using System.IO;
using System.Runtime.Serialization;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System.Data;
using System.Xml;
using Microsoft.Win32;
using System.ServiceProcess;
using System.Diagnostics;
using System.Web.Script.Serialization;

namespace DynamicCRM2Oracle
{
    public class DynamicCRM2Oracle : HybridServiceBase
    {
        static MessageQueue mq;
        static bool stop = false;
        static OracleConnection conn;
        static List<Entity> dsRecord = new List<Entity>();
        static List<string> dsMessage = new List<string>();
        //private System.Diagnostics.EventLog eventLog1;
        public DynamicCRM2Oracle()
        {
            //eventLog1 = new System.Diagnostics.EventLog();
            //if (!System.Diagnostics.EventLog.SourceExists("DynamicCRM2Oracle"))
            //{
            //    System.Diagnostics.EventLog.CreateEventSource(
            //        "DynamicCRM2Oracle", "DynamicCRM2OracleLog");
            //}
            //eventLog1.Source = "DynamicCRM2Oracle";
            //eventLog1.Log = "DynamicCRM2OracleLog";
        }
        protected override void DoWork()
        {

        }
        protected override void OnStop()
        {
            stop = true;
            base.OnStop();
        }

        protected override void OnPause()
        {
            stop = true;
            base.OnPause();
        }

        protected override void OnContinue()
        {
            stop = false;
            var worker = new Thread(BeginETL);
            worker.Name = "ETLWork";
            worker.IsBackground = false;
            worker.Start();
            base.OnContinue();
        }

        protected override void OnStart(string[] args)
        {
            //try
            //{
            Console.WriteLine("start services !");
            //eventLog1.WriteEntry("In OnStart");
            // Set up a timer to trigger every minute.
            System.Timers.Timer timer = new System.Timers.Timer();
            timer.Interval = 60000; // 60 seconds
            timer.Elapsed += new System.Timers.ElapsedEventHandler(this.OnTimer);
            timer.Start();

            stop = false;

            XmlDocument xdoc = new XmlDocument();
            var currentPath = Directory.GetParent(Directory.GetCurrentDirectory()).Parent;
            xdoc.Load("config.xml");
            string host = xdoc.GetElementsByTagName("Host").Count > 0 ? xdoc.GetElementsByTagName("Host")[0].InnerText : "";
            string port = xdoc.GetElementsByTagName("Port").Count > 0 ? xdoc.GetElementsByTagName("Port")[0].InnerText : "1521";
            string service = xdoc.GetElementsByTagName("Service").Count > 0 ? xdoc.GetElementsByTagName("Service")[0].InnerText : "";
            string user = xdoc.GetElementsByTagName("UserName").Count > 0 ? xdoc.GetElementsByTagName("UserName")[0].InnerText : "";
            string pass = xdoc.GetElementsByTagName("Password").Count > 0 ? xdoc.GetElementsByTagName("Password")[0].InnerText : "";
            string oradb = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=" + host + ")(PORT=" + port + ")))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=" + service + ")));User Id = " + user + "; Password = " + pass + "; ";
            //string oradb = "SERVER=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=" + host + ")(PORT=" + port + "))(CONNECT_DATA=(SERVICE_NAME=" + service + ")));uid=" + user + ";pwd=" + pass + ";";

            Console.WriteLine("get config !");
            Console.WriteLine(oradb);
            conn = new OracleConnection(oradb);
            conn.Open();
            OracleCommand cmd = new OracleCommand("Select count(*) from ERPTEST.FRM_SUPPLIER_INT_T ", conn);
            cmd.ExecuteNonQuery();
            conn.Close();

            Console.WriteLine("test connection !");
            var worker = new Thread(BeginETL);
            worker.Name = "ETLWork";
            worker.IsBackground = false;
            worker.Start();

            base.OnStart(args);
            //}
            //catch (Exception ex)
            //{

            //Console.WriteLine(ex.Source + "\n\n" + ex.Message + "\n\n" + ex.StackTrace);

            //System.Diagnostics.EventLog appLog = new System.Diagnostics.EventLog();
            //appLog.Source = "Dynamic CRM 2 Oracle Services";
            //appLog.WriteEntry(ex.Message, System.Diagnostics.EventLogEntryType.Error);
            //}
        }

        public void OnTimer(object sender, System.Timers.ElapsedEventArgs args)
        {
            // TODO: Insert monitoring activities here.
            //eventLog1.WriteEntry("Monitoring the System", EventLogEntryType.Information);
        }

        void BeginETL()
        {
            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle"))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle");
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle");
            mq.Formatter = new System.Messaging.XmlMessageFormatter(new string[] { "System.String,mscorlib" });

            while (!stop)
            {
                bool co = true;
                while (co)
                {
                    #region Start while
                    try
                    {
                        #region Start try
                        Console.WriteLine("waiting get...");
                        Cursor cursor = mq.CreateCursor();
                        Message m = mq.Peek(MessageQueue.InfiniteTimeout, cursor, PeekAction.Current);
                        Console.WriteLine("get 1 message: " + m.Label);

                        if (m.Label == "cust") //insert customer
                        {
                            Console.WriteLine(m.Label + ":body");
                            Entity customer = (Entity)Deserialize(m.Body.ToString(), typeof(Entity));
                            //Console.WriteLine(m.Body.ToString());
                            if (insertCustomer(customer))
                                mq.ReceiveById(m.Id);
                            else
                                Thread.Sleep(300);
                            co = false;
                        }
                        else if (m.Label == "bank") //insert bankaccount
                        {
                            Entity bank = (Entity)Deserialize(m.Body.ToString(), typeof(Entity));
                            if (insertBankAccount(bank))
                                mq.ReceiveById(m.Id);
                            else
                                Thread.Sleep(300);
                            co = false;
                        }
                        else
                        {
                            if (m.Label == "invo") //etl transaction
                            {
                                Entity result = (Entity)Deserialize(m.Body.ToString(), typeof(Entity));
                                dsRecord.Add(result);
                                dsMessage.Add(m.Id);
                                Console.WriteLine("Cast ok !");
                                bool co1 = true;
                                while (co1)
                                {
                                    try
                                    {
                                        Message m2 = mq.Peek(MessageQueue.InfiniteTimeout, cursor, PeekAction.Next);
                                        Console.WriteLine("get 1 sub message: " + m2.Label);
                                        if (m2.Label == "brek")
                                        {
                                            Console.WriteLine("Insert list ! " + dsRecord.Count);
                                            if (insertInvoice(dsRecord))
                                            {
                                                foreach (string a in dsMessage)
                                                    mq.ReceiveById(a);
                                                mq.ReceiveById(m2.Id);
                                            }
                                            co1 = false;
                                            cursor.Close();
                                            dsMessage.Clear();
                                            dsRecord.Clear();
                                        }
                                        else
                                        {
                                            Entity result2 = (Entity)Deserialize(m2.Body.ToString(), typeof(Entity));
                                            Console.WriteLine("Cast ok !");
                                            dsRecord.Add(result2);
                                            dsMessage.Add(m2.Id);
                                        }
                                    }
                                    catch (Exception ex2)
                                    {
                                        co1 = false;
                                        cursor.Close();
                                        dsMessage.Clear();
                                        dsRecord.Clear();
                                        Console.WriteLine(ex2.Message);
                                        Console.WriteLine(ex2.StackTrace);
                                        Console.ReadLine();
                                    }

                                }
                            }
                            else //end transaction
                            {
                                Console.WriteLine("Chua co case : " + m.Label);
                                Console.ReadLine();
                            }
                        }
                        #endregion
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                        Console.ReadLine();
                    }
                    #endregion
                }
            }
        }

        public static string Serialize(object obj)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(obj.GetType());
                serializer.WriteObject(memoryStream, obj);
                memoryStream.Position = 0;
                return reader.ReadToEnd();
            }
        }

        public static object Deserialize(string xml, Type toType)
        {
            using (Stream stream = new MemoryStream())
            {
                byte[] data = System.Text.Encoding.UTF8.GetBytes(xml);
                stream.Write(data, 0, data.Length);
                stream.Position = 0;
                DataContractSerializer deserializer = new DataContractSerializer(toType);
                return deserializer.ReadObject(stream);
            }
        }

        bool insertCustomer(Entity a)
        {
            string tenKh = (a.LogicalName.ToLower() == "contact" ? (a.Contains("fullname") ? a["fullname"].ToString() : "") : (a.Contains("name") ? a["name"].ToString() : ""));
            string cmnd = (a.LogicalName.ToLower() == "contact" ? (a.Contains("new_socmnd") ? a["new_socmnd"].ToString() : "") : (a.Contains("new_masothue") ? a["new_masothue"].ToString() : ""));
            string s1 = StringUtil.RemoveSign4VietnameseString(tenKh).ToUpper() + "_" + cmnd;
            //Console.WriteLine("ma KH: " + a["new_makhachhang"].ToString());
            string maKH = (a.Contains("new_makhachhang") ? a["new_makhachhang"].ToString() : "");
            string address = (a.Contains("new_diachithuongtru") ? ((EntityReference)a["new_diachithuongtru"]).Name : "");

            OracleCommand cmd = new OracleCommand("Insert into ERPTEST.FRM_SUPPLIER_INT_T (Supplier_Name, Alternate_Name, Supplier_Type_Code, " +
               " FRM_Registration_Num, FRM_Customer_ID, Country_Code, Address, City, Site_Name) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)", conn);
            cmd.Parameters.Add(new OracleParameter("1", OracleDbType.Varchar2, s1, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("2", OracleDbType.Varchar2, tenKh, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("3", OracleDbType.Varchar2, "FARMERS", ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("4", OracleDbType.Varchar2, cmnd, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("5", OracleDbType.Varchar2, maKH, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("6", OracleDbType.Varchar2, "VN", ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("7", OracleDbType.Varchar2, address, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("8", OracleDbType.Varchar2, "Tây Ninh", ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("9", OracleDbType.Varchar2, "TAY NINH", ParameterDirection.Input));

            conn.Open();
            int row = cmd.ExecuteNonQuery();
            conn.Close();
            if (row == 0)
                return false;
            else
                return true;
        }

        bool insertBankAccount(Entity a)
        {
            string tenKh = (a.Contains("new_khachhang") ? ((EntityReference)a["new_khachhang"]).Name : ((EntityReference)a["new_khachhangdoanhnghiep"]).Name);
            string cmnd = (a.Contains("cmnd") ? a["cmnd"].ToString() : "");
            string s1 = StringUtil.RemoveSign4VietnameseString(tenKh).ToUpper() + "_" + cmnd;
            string maKH = (a.Contains("new_makhachhang") ? a["new_makhachhang"].ToString() : "");
            string bank = (a.Contains("new_nganhang") ? ((EntityReference)a["new_nganhang"]).Name : "");
            string bankbrand = (a.Contains("new_chinhanh") ? ((EntityReference)a["new_chinhanh"]).Name : "");
            string sotk = (a.Contains("new_sotaikhoan") ? a["new_sotaikhoan"].ToString() : "");
            string chutk = (a.Contains("new_chutaikhoan") ? a["new_chutaikhoan"].ToString() : "");

            OracleCommand cmd = new OracleCommand("Insert into ERPTEST.FRM_SUPPLIER_BANKACC_INT_T ( FRM_Customer_ID, Supplier_Name, Country_Code, Bank_Name, Bank_Branch, " +
               " Bank_Account_Num, Bank_Account_Name, Currency_Code) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", conn);
            cmd.Parameters.Add(new OracleParameter("1", OracleDbType.Varchar2, maKH, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("2", OracleDbType.Varchar2, s1, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("3", OracleDbType.Varchar2, "VN", ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("4", OracleDbType.Varchar2, bank, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("5", OracleDbType.Varchar2, bankbrand, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("6", OracleDbType.Varchar2, sotk, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("7", OracleDbType.Varchar2, chutk.ToUpper(), ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("8", OracleDbType.Varchar2, "VND", ParameterDirection.Input));
            conn.Open();
            int row = cmd.ExecuteNonQuery();
            conn.Close();
            if (row == 0)
                return false;
            else
                return true;
        }

        bool insertInvoice(List<Entity> data)
        {
            conn.Open();
            OracleTransaction trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

            foreach (Entity a in data)
            {
                Console.WriteLine(Serialize(a));
                if (a.LogicalName.Trim().ToLower() == "new_etltransaction")
                {
                    string tenKh = a.Contains("name") ? a["name"].ToString() : "";
                    string cmnd = a.Contains("new_socmnd") ? a["new_socmnd"].ToString() : "";
                    string s1 = StringUtil.RemoveSign4VietnameseString(tenKh).ToUpper() + "_" + cmnd;
                    string maKH = (a.Contains("new_makhachhang") ? a["new_makhachhang"].ToString() : "");
                    string invoiceNum = a.Contains("new_name") ? a["new_name"].ToString() : "";
                    string lenhChi = "";
                    if (!string.IsNullOrEmpty(invoiceNum) && invoiceNum.StartsWith("LC"))
                    {
                        lenhChi = invoiceNum.Split('_')[0];
                    }
                    Console.WriteLine("maKH: " + maKH);
                    //string address = (a.Contains("new_diachithuongtru") ? ((EntityReference)a["new_diachithuongtru"]).Name : "");

                    string tran_type = a["tran_type"].ToString().Trim();

                    if (tran_type.ToUpper() == "CRE")
                    {
                        tran_type = "Credit Memo";
                    }
                    else if (tran_type.ToUpper() == "STA")
                    {
                        tran_type = "Standard";
                    }
                    else if (tran_type.ToUpper() == "PRE")
                    {
                        tran_type = "Prepayment";
                    }
                    else if (tran_type.ToUpper() == "MIX")
                        tran_type = "Mixed";
                    else tran_type = "";

                    OracleCommand cmd = new OracleCommand("Insert into ERPTEST.FRM_TRANSACTION_INT_T  (" +
                        "Invoice_Number, " + //1
                        "Invoice_Type_Des, " + //2
                        "Transaction_Type, " + //3
                        "Customer_Type, " + //4
                        "Season, " + //5
                        "Contract_Number, " + //6
                        "Document_No, " + //7
                        "Transaction_Line, " +//8
                        "Supplier_Name, " + //9
                        "FRM_Customer_ID, " + //10
                        "Invoice_Date, " + //11
                        "GL_Date, " + //12
                        "Description_Header, " +//13
                        "Voucher_Number, " +//14
                        "Terms_Name, " +//15
                        "Tax_Invoice_Type, " +//16
                        "Tax_Type, " +//17
                        "Invoice_Amount, " +//18
                        "Payment_Type, " +//19
                        "Description_Lines, " +//20
                        "INVOICE_BATCH, " + //21
                        "PAYMENT_NO, " + //22
                        "PAYMENT_TYPE_DES)"//23
                        + " VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23)", conn);
                    cmd.Parameters.Add(new OracleParameter("1", OracleDbType.Varchar2, a.Contains("new_name") ? a["new_name"].ToString() : "", ParameterDirection.Input));//Invoice_Number
                    cmd.Parameters.Add(new OracleParameter("2", OracleDbType.Varchar2, tran_type, ParameterDirection.Input));//Invoice_Type_Des
                    cmd.Parameters.Add(new OracleParameter("3", OracleDbType.Varchar2, a.Contains("new_transactiontype") ? a["new_transactiontype"].ToString() : "", ParameterDirection.Input)); // Transaction_Type
                    cmd.Parameters.Add(new OracleParameter("4", OracleDbType.Varchar2, "FARMERS", ParameterDirection.Input)); //Customer_Type
                    cmd.Parameters.Add(new OracleParameter("5", OracleDbType.Varchar2, a.Contains("new_season") ? a["new_season"].ToString() : "", ParameterDirection.Input));//Season

                    cmd.Parameters.Add(new OracleParameter("6", OracleDbType.Varchar2, a.Contains("new_contractnumber") ? a["new_contractnumber"].ToString() : "", ParameterDirection.Input)); //Contract_Number
                    cmd.Parameters.Add(new OracleParameter("7", OracleDbType.Varchar2, a.Contains("new_sochungtu") ? a["new_sochungtu"].ToString() : "", ParameterDirection.Input));//Document_No
                    cmd.Parameters.Add(new OracleParameter("8", OracleDbType.Varchar2, a.Contains("new_lannhan") ? a["new_lannhan"].ToString() : "1", ParameterDirection.Input));//Transaction_Line
                    cmd.Parameters.Add(new OracleParameter("9", OracleDbType.Varchar2, s1, ParameterDirection.Input));//Supplier_Name
                    cmd.Parameters.Add(new OracleParameter("10", OracleDbType.Varchar2, maKH, ParameterDirection.Input));//FRM_Customer_ID

                    cmd.Parameters.Add(new OracleParameter("11", OracleDbType.Date, a.Contains("new_invoicedate") ? (DateTime?)((DateTime)a["new_invoicedate"]).AddHours(7).Date : null, ParameterDirection.Input));//Invoice_Date
                    cmd.Parameters.Add(new OracleParameter("12", OracleDbType.Date, a.Contains("new_gldate") ? (DateTime?)((DateTime)a["new_gldate"]).AddHours(7).Date : null, ParameterDirection.Input));// GL Date
                    cmd.Parameters.Add(new OracleParameter("13", OracleDbType.Varchar2, a.Contains("new_descriptionheader") ? a["new_descriptionheader"].ToString() : "", ParameterDirection.Input));//Description_Header
                    cmd.Parameters.Add(new OracleParameter("14", OracleDbType.Varchar2, a.Contains("new_vouchernumber") ? a["new_vouchernumber"].ToString() : "", ParameterDirection.Input));//Voucher_Number
                    cmd.Parameters.Add(new OracleParameter("15", OracleDbType.Varchar2, a.Contains("new_terms") ? a["new_terms"].ToString() : "", ParameterDirection.Input));//Terms_Name

                    cmd.Parameters.Add(new OracleParameter("16", OracleDbType.Varchar2, "", ParameterDirection.Input));//Tax_Invoice_Type
                    cmd.Parameters.Add(new OracleParameter("17", OracleDbType.Varchar2, a.Contains("new_taxtype") ? a["new_taxtype"].ToString() : "", ParameterDirection.Input));//Tax_Type
                    cmd.Parameters.Add(new OracleParameter("18", OracleDbType.Varchar2, a.Contains("new_invoiceamount") ? ((Money)a["new_invoiceamount"]).Value.ToString() : "0", ParameterDirection.Input));//Invoice_Amount
                    cmd.Parameters.Add(new OracleParameter("19", OracleDbType.Varchar2, a.Contains("new_paymenttype") ? a["new_paymenttype"].ToString() : "", ParameterDirection.Input));//Payment_Type
                    cmd.Parameters.Add(new OracleParameter("20", OracleDbType.Varchar2, a.Contains("new_descriptionlines") ? a["new_descriptionlines"].ToString() : "", ParameterDirection.Input));//Description_Lines

                    cmd.Parameters.Add(new OracleParameter("21", OracleDbType.Varchar2, "DT_CANTRU_2015", ParameterDirection.Input));//Description_Lines      
                    cmd.Parameters.Add(new OracleParameter("22", OracleDbType.Varchar2, lenhChi, ParameterDirection.Input));
                    cmd.Parameters.Add(new OracleParameter("23", OracleDbType.Varchar2, a.Contains("new_descriptionheader") ? a["new_descriptionheader"].ToString() : "", ParameterDirection.Input));

                    cmd.Transaction = trans;
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    string tenKh = a.Contains("name") ? a["name"].ToString() : "";
                    string cmnd = a.Contains("new_socmnd") ? a["new_socmnd"].ToString() : "";
                    string s1 = StringUtil.RemoveSign4VietnameseString(tenKh).ToUpper() + "_" + cmnd;
                    string maKH = (a.Contains("new_makhachhang") ? a["new_makhachhang"].ToString() : "");

                    OracleCommand cmd = new OracleCommand("Insert into ERPTEST.FRM_APPLY_PAYMENT_INT_T  (" +
                        "APPLY_ID, " + //1
                        "Supplier_Name, " +//2
                        "FRM_Customer_ID, " +//3
                        "Payment_Date, " +//4
                        "Bank_Account_Name, " +//5
                        "Document_No, " +//6
                        "Description, " +//7
                        "Voucher_Number, " +//8
                        "Receiver_Sender, " +//9
                        "Address, " +//10
                        "Originial_Document, " +//11
                        "Cash_Flow, " +//12
                        "Invoice_Num, " +//13
                        "Payment_Amount, " +//14
                        "SUPPLIER_BANK_NUM, " +//15
                        "Reference_Num, " + //16
                        "Payment_num, " + //17
                        "TYPE) "//18
                        + " VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18)", conn);
                    cmd.Parameters.Add(new OracleParameter("1", OracleDbType.Varchar2, a.Id, ParameterDirection.Input));//ApplyId
                    cmd.Parameters.Add(new OracleParameter("2", OracleDbType.Varchar2, s1, ParameterDirection.Input));//Supplier_Name
                    cmd.Parameters.Add(new OracleParameter("3", OracleDbType.Varchar2, maKH, ParameterDirection.Input)); // FRM_Customer_ID
                    cmd.Parameters.Add(new OracleParameter("4", OracleDbType.Date, a.Contains("new_paymentdate") ? (DateTime?)((DateTime)a["new_paymentdate"]).AddHours(7).Date : null, ParameterDirection.Input)); //Payment_Date
                    cmd.Parameters.Add(new OracleParameter("5", OracleDbType.Varchar2, a.Contains("new_bankcccountnum") ? a["new_bankcccountnum"].ToString() : "", ParameterDirection.Input));//Bank_Account_Name

                    cmd.Parameters.Add(new OracleParameter("6", OracleDbType.Varchar2, a.Contains("new_documentnum") ? a["new_documentnum"].ToString() : "", ParameterDirection.Input)); //Document_No
                    cmd.Parameters.Add(new OracleParameter("7", OracleDbType.Varchar2, "", ParameterDirection.Input));//Description
                    cmd.Parameters.Add(new OracleParameter("8", OracleDbType.Varchar2, a.Contains("new_vouchernumber") ? a["new_vouchernumber"].ToString() : "", ParameterDirection.Input));//Voucher_Number
                    cmd.Parameters.Add(new OracleParameter("9", OracleDbType.Varchar2, "", ParameterDirection.Input));//Receiver/Sender
                    cmd.Parameters.Add(new OracleParameter("10", OracleDbType.Varchar2, "", ParameterDirection.Input));//Address

                    cmd.Parameters.Add(new OracleParameter("11", OracleDbType.Varchar2, "", ParameterDirection.Input));//Originial_Document
                    cmd.Parameters.Add(new OracleParameter("12", OracleDbType.Varchar2, a.Contains("new_cashflow") ? a["new_cashflow"].ToString() : "", ParameterDirection.Input));// Cash_Flow
                    cmd.Parameters.Add(new OracleParameter("13", OracleDbType.Varchar2, a.Contains("new_name") ? a["new_name"].ToString() : "", ParameterDirection.Input));//Invoice_Num
                    cmd.Parameters.Add(new OracleParameter("14", OracleDbType.Decimal, a.Contains("new_paymentamount") ? (a["new_paymentamount"] as Money) != null ? (a["new_paymentamount"] as Money).Value : 0 : 0, ParameterDirection.Input));//Payment_Amount
                    cmd.Parameters.Add(new OracleParameter("15", OracleDbType.Varchar2, a.Contains("new_supplierbankname") ? a["new_supplierbankname"].ToString() : "", ParameterDirection.Input));//Supplier_Bank_Name

                    cmd.Parameters.Add(new OracleParameter("16", OracleDbType.Varchar2, a.Contains("new_referencenumber") ? a["new_referencenumber"].ToString() : "", ParameterDirection.Input));//Reference_Num
                    cmd.Parameters.Add(new OracleParameter("17", OracleDbType.Varchar2, "1", ParameterDirection.Input));//Reference_Num
                    cmd.Parameters.Add(new OracleParameter("18", OracleDbType.Varchar2, a.Contains("new_type") ? a["new_type"].ToString() : "", ParameterDirection.Input));//Type

                    cmd.Transaction = trans;
                    cmd.ExecuteNonQuery();
                }
            }

            try
            {
                trans.Commit();
                conn.Close();
                return true;
            }
            catch
            {
                trans.Rollback();
                conn.Close();
                return false;
            }
        }

    }
}
