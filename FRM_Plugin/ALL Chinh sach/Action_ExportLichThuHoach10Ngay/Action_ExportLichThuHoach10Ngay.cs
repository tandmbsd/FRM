using System;
using System.ServiceModel.Description;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk.Messages;
using System.Xml;
using System.Text;
using System.IO;
using System.Security;
using System.Runtime.Remoting.Contexts;
using System.Runtime.InteropServices;
using ExcelLibrary.SpreadSheet;

namespace Action_ExportLichThuHoach10Ngay
{
    public class Action_ExportLichThuHoach10Ngay : IPlugin
    {
        IOrganizationService orgService = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        { 
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            orgService = factory.CreateOrganizationService(context.UserId);
            try
            {
                Guid file = WriteContactsToExcel(target.Id);
                if (file == Guid.Empty)
                    context.OutputParameters["ReturnId"] = "";
                else context.OutputParameters["ReturnId"] = file.ToString();
            }
            catch (Exception ex)
            {
                context.OutputParameters["ReturnId"] = ex.Message;
            }
        }

        public Guid WriteContactsToExcel(Guid vth)
        {
            // fetch XML
            var fetchXml = @"<fetch mapping='logical' aggregate='true' version='1.0'> distinct='false'" +
              "  <entity name='new_lichthuhoach10ngay'>" +
              "    <attribute name='new_tram' groupby='true' alias='new_tram' descending='true'/>" +
              "    <attribute name='new_dotthuhoach' groupby='true' alias='new_dotthuhoach' descending='true'/>" +
              "    <attribute name='new_sanluong' alias='SUM_new_sanluong' aggregate='sum' />" +
              "    <filter type='and'>" +
              "      <condition attribute='new_vuthuhoach' operator='eq' value='" + vth + "' />" +
              "    </filter>" +
              "  </entity>" +
              "</fetch>";
            var fetchxml1 = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>" +
                             "  <entity name='new_dotthuhoach'>" +
                             "    <attribute name='new_dotthuhoachid' />" +
                             "    <attribute name='new_name' />" +
                             "    <attribute name='new_tungay' />" +
                             "    <attribute name='new_denngay' />" +
                             "    <attribute name='new_songayhoatdong' />" +
                             "    <order attribute='new_name' descending='false' />" +
                             "    <order attribute='new_tungay' descending='false' />" +
                             "    <filter type='and'>" +
                             "      <condition attribute='new_vuthuhoach' operator='eq'  value='" + vth + "' />" +
                             "    </filter>" +
                             "  </entity>" +
                             "</fetch>";
            fetchXml = string.Format(fetchXml);

            EntityCollection entc = orgService.RetrieveMultiple(new FetchExpression(fetchXml));
            fetchxml1 = string.Format(fetchxml1);
            EntityCollection entc1 = orgService.RetrieveMultiple(new FetchExpression(fetchxml1));

            // sắp xếp đợt thu hoạch
            #region tính toán
            Dictionary<Guid, string> list1 = new Dictionary<Guid, string>();
            Dictionary<Guid, string> list2 = new Dictionary<Guid, string>();
            foreach (Entity a in entc1.Entities)
            {
                if (a["new_name"].ToString().Length == 5)
                {
                    list1.Add(a.Id, a["new_name"].ToString());
                }
                else
                    list2.Add(a.Id, a["new_name"].ToString());
            }
            if (list1.Count > 0 || list2.Count > 0)
            {
                foreach (var item in list2)
                {
                    Guid k = Guid.Parse(item.Key.ToString());
                    string val = item.Value.ToString();
                    list1.Add(k, val);
                }
            }
            // tạo list tram
            HashSet<string> list_tram = new HashSet<string>();
            Dictionary<Guid, string> lst_tram = new Dictionary<Guid, string>();

            for (int j = 0; j < entc.Entities.Count; j++)
            {
                Guid id_tram = ((EntityReference)((AliasedValue)entc.Entities[j].Attributes["new_tram"]).Value).Id;
                Entity tram = orgService.Retrieve(((EntityReference)((AliasedValue)entc.Entities[j].Attributes["new_tram"]).Value).LogicalName, id_tram, new ColumnSet(new string[] { "name" }));
                list_tram.Add(tram["name"].ToString());
                string key = null;
                key = (lst_tram.FirstOrDefault(x => x.Key == id_tram).Key).ToString();
                if (key == "00000000-0000-0000-0000-000000000000")
                {
                    lst_tram.Add(id_tram, tram["name"].ToString());
                }
            }
            // sum theo dot thu hoach
            List<int> sum_dotthuhoach = new List<int>();
            for (int i = 0; i < list1.Count; i++)
            {
                int sum_dot = 0;
                for (int j = 0; j < entc.Entities.Count; j++)
                {
                    Guid dot_id = ((EntityReference)((AliasedValue)entc.Entities[j].Attributes["new_dotthuhoach"]).Value).Id;
                    if (list1.Keys.ToList()[i] == dot_id)
                    {
                        sum_dot += Int32.Parse((((AliasedValue)entc.Entities[j].Attributes["SUM_new_sanluong"]).Value).ToString());
                    }
                }
                sum_dotthuhoach.Add(sum_dot);
            }
            // tính số ngày hoạt động
            Dictionary<string, int> list_ngayhd = new Dictionary<string, int>();
            for (int i = 0; i < entc1.Entities.Count; i++)
            {
                list_ngayhd.Add(entc1.Entities[i].Attributes["new_name"].ToString(), Int32.Parse(entc1.Entities[i].Attributes["new_songayhoatdong"].ToString()));
            }

            //Create Excel object
            Workbook workbook = new Workbook();





            #endregion

            // --------------------------------------------SHEET 1 ----------------------------------------------------
            #region
            Worksheet worksheet = new Worksheet("Phu luc 1");
            //Create Excel Sheet Header Columns//mentioned as in FetchXML
            Entity vt = orgService.Retrieve("new_vuthuhoach", vth, new ColumnSet(new string[] { "new_name" }));

            string tieude = vt.Attributes["new_name"].ToString();
            worksheet.Cells[0, 0] = new Cell("TỔNG HỢP LỊCH THU HOẠCH VỤ ÉP " + tieude.ToUpper());

            worksheet.Cells[2, 0] = new Cell("Trạm");
            worksheet.Cells[2, 1] = new Cell("USL mía \n nguyên \n liệu");
            worksheet.Cells[2, 2] = new Cell("Lịch thu hoạch đến cuối vụ");

            //maxColValue[column] = row[column].ToString().Length * 500;
            worksheet.Cells.ColumnWidth[(ushort)0] = (ushort)4000;
            worksheet.Cells.ColumnWidth[(ushort)1] = (ushort)5000;
            //NsExcel.Range range = worksheet.get_Range("A1", "A3");
            //NsExcel.Range range2 = worksheet.get_Range("B1", "B3");
            //NsExcel.Range range3 = worksheet.get_Range("C1", alphabet[entc1.Entities.Count+1]+"1");

            //  range.Interior.ColorIndex = 36;
            //range.Merge();
            //range2.Merge();
            //range3.Merge();
            //worksheet.Columns[1].ColumnWidth = 20;
            //worksheet.Columns[2].ColumnWidth = 10;
            // gen đợt thu hoạch
            for (int j = 0; j < list1.Count; j++)
            {
                worksheet.Cells.ColumnWidth[(ushort)(j + 2)] = (ushort)3000;
                //ushort postition = (ushort)(j+2);

                //worksheet.Cells.ColumnWidth[0, postition] = 1300;
                // tạo đợt
                worksheet.Cells[2, (2 + j)] = new Cell(list1.Values.ToList()[j]);
                // tạo ngày 
                foreach (Entity a in entc1.Entities)
                {
                    if (a["new_name"].ToString() == list1.Values.ToList()[j])
                    {
                        DateTime tungay_full = (DateTime)a["new_tungay"];
                        DateTime denngay_full = (DateTime)a["new_denngay"];
                        int tungay_ngay = tungay_full.Day;
                        int denngay_ngay = denngay_full.Day;
                        int tungay_thang = tungay_full.Month;
                        int denngay_thang = denngay_full.Month;

                        worksheet.Cells[3, (2 + j)] = new Cell("(" + tungay_ngay + "/" + tungay_thang + "-" + "\n" + denngay_ngay + "/" + denngay_thang + ")");
                    }
                }
            }


            // tạo nội dung
            int ps = 4;
            int sum_all = 0;
            foreach (var i in list_tram)
            {
                int sum_tram = 0;
                for (int j = 0; j < entc.Entities.Count; j++)
                {

                    Guid id_tram = ((EntityReference)((AliasedValue)entc.Entities[j].Attributes["new_tram"]).Value).Id;
                    string tram = lst_tram.FirstOrDefault(x => x.Key == id_tram).Value;
                    Guid id_dot = ((EntityReference)((AliasedValue)entc.Entities[j].Attributes["new_dotthuhoach"]).Value).Id;
                    string dotthuhoach = list1.FirstOrDefault(x => x.Key == id_dot).Value;
                    if (i == tram)
                    {
                        for (int k = 0; k < list1.Count; k++)
                        {
                            if (list1.Values.ToList()[k] == dotthuhoach)
                            {
                                decimal vl_cell = System.Convert.ToDecimal((((AliasedValue)entc.Entities[j].Attributes["SUM_new_sanluong"]).Value).ToString());
                                worksheet.Cells[ps, k + 2] = new Cell((decimal)vl_cell, "#,##");

                            }
                        }
                        worksheet.Cells[ps, 0] = new Cell(i);
                        sum_tram += Int32.Parse((((AliasedValue)entc.Entities[j].Attributes["SUM_new_sanluong"]).Value).ToString());
                    }
                }
                sum_all += sum_tram;
                worksheet.Cells[ps, 1] = new Cell(sum_tram, "#,##");
                ps++;
                Console.WriteLine(i);
            }
            worksheet.Cells[list_tram.Count + 4, 0] = new Cell("Tổng SL");
            worksheet.Cells[list_tram.Count + 4, 1] = new Cell(sum_all, "#,##");
            worksheet.Cells[list_tram.Count + 5, 0] = new Cell("Số ngày hoạt động");
            worksheet.Cells[list_tram.Count + 6, 0] = new Cell("SL BQ/ngày");

            for (var i = 0; i < sum_dotthuhoach.Count; i++)
            {
                worksheet.Cells[list_tram.Count + 4, i + 1] = new Cell(sum_dotthuhoach[i], "#,##");
            }
            // tạo số ngày hoạt động và sl bq 
            int tongngayhd = 0;
            int ps_hd = 2;
            for (int j = 0; j < list1.Count; j++)
            {
                // tạo số ngày hoạt động
                for (int k = 0; k < list_ngayhd.Count; k++)
                {
                    if (list_ngayhd.Keys.ToList()[k] == list1.Values.ToList()[j])
                    {
                        worksheet.Cells[list_tram.Count + 5, ps_hd] = new Cell(list_ngayhd.Values.ToList()[k]);
                        tongngayhd += list_ngayhd.Values.ToList()[k];
                        worksheet.Cells[list_tram.Count + 6, ps_hd] = new Cell(sum_dotthuhoach[j] / list_ngayhd.Values.ToList()[k], "#,##");
                    }
                }
                ps_hd++;
            }
            worksheet.Cells[list_tram.Count + 5, 1] = new Cell(tongngayhd);
            worksheet.Cells[list_tram.Count + 6, 1] = new Cell(sum_all / tongngayhd, "#,##");


            #endregion

            workbook.Worksheets.Add(worksheet);
            //--------------------------------------SHEEET 2 --------------------------------------------------------

            #region
            var fetchxml_lichchitiet = "<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>" +
                                      "  <entity name='new_lichthuhoach10ngay'>" +
                                      "    <attribute name='new_lichthuhoach10ngayid' />" +
                                      "    <attribute name='new_name' />" +
                                      "    <attribute name='new_tram' />" +
                                      "    <attribute name='new_tuoimia' />" +
                                      "    <attribute name='new_sanluong' />" +
                                      "    <attribute name='new_dotthuhoach' />" +
                                      "    <attribute name='new_lydothuhoachsom' />" +
                                      "    <order attribute='new_tram' descending='false' />" +
                                      "    <filter type='and'>" +
                                      "      <condition attribute='new_vuthuhoach' operator='eq' value='" + vth + "' />" +
                                      //"      <condition attribute='new_tram' operator='eq' value='" + tramid + "' />" +
                                      "    </filter>" +
                                      "    <link-entity name='new_thuadatcanhtac' from='new_thuadatcanhtacid' to='new_thuadatcanhtac' alias='aa'>" +
                                      "      <attribute name='new_thuadat' />" +
                                      //"      <attribute name='new_thuadatcanhtacid' />" +

                                      "      <attribute name='new_ngaytrong' />" +
                                      "      <attribute name='new_khachhangdoanhnghiep' />" +
                                      "      <attribute name='new_khachhang' />" +
                                      //"      <attribute name='new_hopdongdaututhuedat' />" +
                                      "      <attribute name='new_giongtrongthucte' />" +
                                      "      <attribute name='new_dientichconlai' />" +
                                      "      <attribute name='new_canbonongvu' />" +
                                      "      <attribute name='new_loaigocmia' />" +
                                      "    </link-entity>" +

                                      //"    <link-entity name='contact' from='contactid' to='new_khachhang' alias='ab'>" +
                                      //"      <attribute name='new_socmnd' />" +
                                      //"    </link-entity>" +

                                      "  </entity>" +
                                      "</fetch>";
            fetchxml1 = string.Format(fetchxml_lichchitiet);

            Worksheet worksheet2 = new Worksheet("Phu luc 2");
            Entity vt2 = orgService.Retrieve("new_vuthuhoach", vth, new ColumnSet(new string[] { "new_name" }));

            string tieude2 = vt.Attributes["new_name"].ToString();
            worksheet2.Cells[1, 0] = new Cell("CHI TIẾT LỊCH " + tieude.ToUpper() + "  ĐẾN CUỐI VỤ");
            worksheet2.Cells[3, 12] = new Cell("Lịch thu hoạch đến cuối vụ");
            worksheet2.Cells[5, 0] = new Cell("Trạm");
            worksheet2.Cells[5, 1] = new Cell("CBNV");
            worksheet2.Cells[5, 2] = new Cell("CMND");
            worksheet2.Cells[5, 3] = new Cell("Họ tên");
            worksheet2.Cells[5, 4] = new Cell("Số HĐ");
            worksheet2.Cells[5, 5] = new Cell("Mã vùng");
            worksheet2.Cells[5, 6] = new Cell("Số thửa");
            worksheet2.Cells[5, 7] = new Cell("Giống mía");
            worksheet2.Cells[5, 8] = new Cell("Diện tích");
            worksheet2.Cells[5, 9] = new Cell("Loại mía");
            worksheet2.Cells[5, 10] = new Cell("Ngày trồng");
            worksheet2.Cells[5, 11] = new Cell("SL mía \n nguyên \n liệu");
            worksheet2.Cells.ColumnWidth[(ushort)0] = (ushort)2000;
            worksheet2.Cells.ColumnWidth[(ushort)1] = (ushort)4000;
            worksheet2.Cells.ColumnWidth[(ushort)2] = (ushort)4000;
            worksheet2.Cells.ColumnWidth[(ushort)3] = (ushort)5000;
            worksheet2.Cells.ColumnWidth[(ushort)10] = (ushort)4000;
            worksheet2.Cells.ColumnWidth[(ushort)11] = (ushort)4000;
            // gen đợt thu hoạch
            for (int i = 0; i < list1.Count; i++)
            {
                worksheet2.Cells[4, i + 12] = new Cell(list1.Values.ToList()[i]);
                worksheet2.Cells.ColumnWidth[(ushort)(i + 12)] = (ushort)3000;
                // tạo ngày 
                foreach (Entity a in entc1.Entities)
                {
                    if (a["new_name"].ToString() == list1.Values.ToList()[i])
                    {
                        DateTime tungay_full = (DateTime)a["new_tungay"];
                        DateTime denngay_full = (DateTime)a["new_denngay"];
                        int tungay_ngay = tungay_full.Day;
                        int denngay_ngay = denngay_full.Day;
                        int tungay_thang = tungay_full.Month;
                        int denngay_thang = denngay_full.Month;

                        worksheet2.Cells[5, i + 12] = new Cell("(" + tungay_ngay + "/" + tungay_thang + "-" + "\n" + denngay_ngay + "/" + denngay_thang + ")");
                    }
                }
            }

            // tính tổng theo đợt và tổng theo trạm thửa
            Dictionary<string, int> sum_dot_sheet2 = new Dictionary<string, int>();
            // sum theo thửa đất
            Dictionary<string, int> sum_thuadat_sheet2 = new Dictionary<string, int>();
            // list thửa đất canh tác
            Dictionary<string, int> list_thuadat = new Dictionary<string, int>();

            // tuổi  mía và nguyên nhân
            worksheet2.Cells[5, list1.Count + 12] = new Cell("Tuổi mía");
            // nguyên nhân thu hoạch sớm
            worksheet2.Cells[5, list1.Count + 13] = new Cell("Nguyên nhân \n thu hoạch \n sớm");
            // tạo nội dung chữ bên trái

            int ps_row = 6;
            int p = 0;
            int testdot = 0;


            //  EntityCollection entc2 = orgService.RetrieveMultiple(new FetchExpression(fetchxml_lichchitiet));
            int page = 1;
            EntityCollection entc2 = new EntityCollection();

            do
            {
                entc2 = orgService.RetrieveMultiple(new FetchExpression(String.Format("<fetch version='1.0' page='{1}' paging-cookie='{0}' count='1000' output-format='xml-platform' mapping='logical' distinct='false'> " +
                     "  <entity name='new_lichthuhoach10ngay'>" +
                                      "    <attribute name='new_lichthuhoach10ngayid' />" +
                                      "    <attribute name='new_name' />" +
                                      "    <attribute name='new_tram' />" +
                                      "    <attribute name='new_tuoimia' />" +
                                      "    <attribute name='new_sanluong' />" +
                                      "    <attribute name='new_dotthuhoach' />" +
                                      "    <attribute name='new_lydothuhoachsom' />" +
                                      "    <order attribute='new_tram' descending='false' />" +
                                      "    <filter type='and'>" +
                                      "      <condition attribute='new_vuthuhoach' operator='eq' value='" + vth + "' />" +
                                      //"      <condition attribute='new_tram' operator='eq' value='" + tramid + "' />" +
                                      "    </filter>" +
                                      "    <link-entity name='new_thuadatcanhtac' from='new_thuadatcanhtacid' to='new_thuadatcanhtac' alias='aa'>" +
                                      "      <attribute name='new_thuadat' />" +
                                      //"      <attribute name='new_thuadatcanhtacid' />" +

                                      "      <attribute name='new_ngaytrong' />" +
                                      "      <attribute name='new_khachhangdoanhnghiep' />" +
                                      "      <attribute name='new_khachhang' />" +
                                      //"      <attribute name='new_hopdongdaututhuedat' />" +
                                      "      <attribute name='new_giongtrongthucte' />" +
                                      "      <attribute name='new_dientichconlai' />" +
                                      "      <attribute name='new_canbonongvu' />" +
                                      "      <attribute name='new_loaigocmia' />" +
                                         "<link-entity name='contact' from='contactid' to='new_khachhang' visible='false' link-type='outer' alias='ab'>" +
                                        "      <attribute name='new_socmnd' />" +
                                        "    </link-entity>" +
                                        "    <link-entity name='new_hopdongdautumia' from='new_hopdongdautumiaid' to='new_hopdongdautumia' visible='false' link-type='outer' alias='ac'>" +
                                        "      <attribute name='new_masohopdong' />" +
                                        "    </link-entity>" +
                                        "    <link-entity name='account' from='accountid' to='new_khachhangdoanhnghiep' visible='false' link-type='outer' alias='ad'>" +
                                        "      <attribute name='new_masothue' />" +
                                        "    </link-entity>" +
                                      "    </link-entity>" +
                                      "  </entity>" +
                    "</fetch>", SecurityElement.Escape(entc2.PagingCookie), page++)));

                // Do something with the results here

                #region content page
                foreach (Entity en in entc2.Entities)
                {
                    if (((EntityReference)en.Attributes["new_dotthuhoach"]).Name.ToString() == "Đợt 1")
                        testdot++;
                    string check_sum_dot = ((EntityReference)en.Attributes["new_dotthuhoach"]).Name.ToString();
                    // sum theo đợt
                    if (sum_dot_sheet2.ContainsKey(check_sum_dot))
                    {
                        sum_dot_sheet2[check_sum_dot] += Int32.Parse(en.Attributes["new_sanluong"].ToString());
                    }
                    else
                    {
                        // sum đợt 
                        sum_dot_sheet2.Add(((EntityReference)en.Attributes["new_dotthuhoach"]).Name, Int32.Parse(en.Attributes["new_sanluong"].ToString()));

                    }
                    // sum theo đợt
                    string check_sum_thuadat = ((EntityReference)((AliasedValue)en.Attributes["aa.new_thuadat"]).Value).Name;
                    if (sum_thuadat_sheet2.ContainsKey(check_sum_thuadat))
                    {
                        sum_thuadat_sheet2[check_sum_thuadat] += Int32.Parse(en.Attributes["new_sanluong"].ToString());
                    }
                    else
                        sum_thuadat_sheet2.Add(((EntityReference)((AliasedValue)en.Attributes["aa.new_thuadat"]).Value).Name, Int32.Parse(en.Attributes["new_sanluong"].ToString()));

                    string check = ((EntityReference)((AliasedValue)en.Attributes["aa.new_thuadat"]).Value).Name + ((EntityReference)en.Attributes["new_tram"]).Name;
                    if (!list_thuadat.ContainsKey(check))
                    {
                        // trạm
                        worksheet2.Cells[ps_row, 0] = new Cell(((EntityReference)en.Attributes["new_tram"]).Name);
                        // cbnv
                        if (en.Contains("aa.new_canbonongvu"))
                        {
                            worksheet2.Cells[ps_row, 1] = new Cell(((EntityReference)((AliasedValue)en.Attributes["aa.new_canbonongvu"]).Value).Name);
                        }
                        if (en.Contains("ad.new_masothue"))
                        {
                            worksheet2.Cells[ps_row, 2] = new Cell(((AliasedValue)(en.Attributes["ad.new_masothue"])).Value);
                        }
                        else
                        {
                            worksheet2.Cells[ps_row, 2] = new Cell(((AliasedValue)(en.Attributes["ab.new_socmnd"])).Value);
                        }
                        // ho ten
                        if (en.Contains("aa.new_khachhang"))
                        {
                            worksheet2.Cells[ps_row, 3] = new Cell(((EntityReference)((AliasedValue)en.Attributes["aa.new_khachhang"]).Value).Name);
                        }
                        else
                            worksheet2.Cells[ps_row, 3] = new Cell(((EntityReference)((AliasedValue)en.Attributes["aa.new_khachhangdoanhnghiep"]).Value).Name);


                        // số HĐ
                        string mahd = ((AliasedValue)(en.Attributes["ac.new_masohopdong"])).Value.ToString();
                        worksheet2.Cells[ps_row, 4] = new Cell(mahd.Substring(4, 6).ToString());
                        // mã vùng
                        string sl = ((EntityReference)((AliasedValue)en.Attributes["aa.new_thuadat"]).Value).Name;
                        worksheet2.Cells[ps_row, 5] = new Cell(sl.Substring(0, 5).ToString());
                        //  số thửa

                        worksheet2.Cells[ps_row, 6] = new Cell(sl.Substring(6, 4).ToString());
                        //  giống mía

                        int giong_mia_value = ((OptionSetValue)((AliasedValue)en.Attributes["aa.new_loaigocmia"]).Value).Value;
                        string tengiongmia = "";
                        switch (giong_mia_value)
                        {
                            case 100000000:
                                {
                                    // mía tơ
                                    tengiongmia = "Mía tơ";
                                    break;
                                }
                            case 100000001:
                                {
                                    // mía gốc
                                    tengiongmia = "Mía gốc";
                                    break;
                                }
                            default:
                                {
                                    tengiongmia = "";
                                    break;
                                }
                        }
                        if (tengiongmia != "")
                        {
                            worksheet2.Cells[ps_row, 9] = new Cell(tengiongmia);
                        }

                        // diện tích
                        worksheet2.Cells[ps_row, 8] = new Cell(((AliasedValue)en.Attributes["aa.new_dientichconlai"]).Value);
                        //loại mía
                        worksheet2.Cells[ps_row, 7] = new Cell(((EntityReference)((AliasedValue)en.Attributes["aa.new_giongtrongthucte"]).Value).Name);
                        // ngày trồng
                        if (en.Contains("aa.new_ngaytrong"))
                        {
                            DateTime date = (DateTime)((AliasedValue)en.Attributes["aa.new_ngaytrong"]).Value;

                            worksheet2.Cells[ps_row, 10] = new Cell(date.ToString("MM dd, yyyy"));
                        }
                        // số lượng mía nguyên liệu
                        list_thuadat.Add(check, ps_row);

                        // add ô sản lượng
                        int ps_record = 0;
                        for (ps_record = 0; ps_record < list1.Count; ps_record++)
                        {
                            if (list1.Values.ToList()[ps_record] == ((EntityReference)en.Attributes["new_dotthuhoach"]).Name)
                            {
                                worksheet2.Cells[ps_row, ps_record + 12] = new Cell(en.Attributes["new_sanluong"], "#,##");
                            }
                        }
                        // tuổi  mía và nguyên nhân

                        if (en.Contains("new_tuoimia"))
                        {
                            float tuoimia = float.Parse(en.Attributes["new_tuoimia"].ToString());
                            //int phannguyen = (int)tuoimia / 1;
                            //float phandu = (int)((((float)tuoimia % 1) * 30) / 1);

                            worksheet2.Cells[ps_row, list1.Count + 12] = new Cell(Math.Round(tuoimia, 1));
                        }

                        // nguyên nhân thu hoạch sớm

                        if (en.Contains("new_lydothuhoachsom"))
                        {
                            worksheet2.Cells[ps_row, list1.Count + 13] = new Cell(((EntityReference)(entc2.Entities[0].Attributes["new_lydothuhoachsom"])).Name);
                        }
                        Console.WriteLine("insert row: " + (ps_row - 6));
                        ps_row++;
                        //if (ps_row == 200)
                        //{
                        //    break;
                        //}
                    }
                    else
                    {
                        int ps_record = 0;
                        for (ps_record = 0; ps_record < list1.Count; ps_record++)
                        {
                            if (list1.Values.ToList()[ps_record] == ((EntityReference)en.Attributes["new_dotthuhoach"]).Name)
                            {
                                break;
                            }
                        }
                        int keysWithMatchingValues = (list_thuadat.Where(x => x.Key == check).Select(x => x.Value).FirstOrDefault());

                        string cellValue = worksheet2.Cells[keysWithMatchingValues, ps_record + 12].ToString();





                        // var check_cell = (worksheet2.Cells[keysWithMatchingValues, alphabet[ps_record + 12]] as NsExcel.Range);

                        if (cellValue != "")
                        {
                            int cell_value = Int32.Parse(cellValue);
                            Console.WriteLine("--------------" + cell_value);
                            worksheet2.Cells[keysWithMatchingValues, ps_record + 12] = new Cell(cell_value + Int32.Parse(en.Attributes["new_sanluong"].ToString()), "#,##");
                        }
                        else
                        {
                            worksheet2.Cells[keysWithMatchingValues, ps_record + 12] = new Cell(en.Attributes["new_sanluong"], "#,##");
                        }
                        // worksheet2.Cells[keysWithMatchingValues, alphabet[ps_record + 10]] = en.Attributes["new_sanluong"];
                    }


                    Console.WriteLine("test: " + p);
                    p++;
                }

                #endregion
            }
            while (entc2.MoreRecords);

            worksheet2.Cells[ps_row, 0] = new Cell("Tổng cộng");
            for (int i = 0; i < list1.Count; i++)
            {
                for (int j = 0; j < sum_dot_sheet2.Count; j++)
                {
                    if (list1.Values.ToList()[i] == sum_dot_sheet2.Keys.ToList()[j])
                        worksheet2.Cells[ps_row, i + 12] = new Cell(sum_dot_sheet2.Values.ToList()[j], "#,##");
                }
            }
            // sum theo thửa đất
            for (int i = 0; i < sum_thuadat_sheet2.Count; i++)
            {
                worksheet2.Cells[i + 6, 11] = new Cell(sum_thuadat_sheet2.Values.ToList()[i], "#,##");
            }
            workbook.Worksheets.Add(worksheet2);
           //workbook.Save(tempFilePath);

            // bold tổng cộng
            //NsExcel.Range range15 = worksheet2.get_Range("J6", "J" + ps_row);

            #endregion

            using (var ms = new MemoryStream())
            {
                workbook.SaveToStream(ms);

                Guid kq = Guid.Empty;
                Entity Annotation = new Entity("annotation");
                Annotation.Attributes["objectid"] = new EntityReference("new_vuthuhoach", vth);
                Annotation.Attributes["objecttypecode"] = "new_vuthuhoach";
                Annotation.Attributes["subject"] = "Lịch thu hoạch 10 ngày " + DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss");
                Annotation.Attributes["documentbody"] = Convert.ToBase64String(ms.ToArray());
                Annotation.Attributes["mimetype"] = @"application/vnd.ms-excel";
                Annotation.Attributes["notetext"] = "file xuất lịch thu hoạch 10 ngày. Ngày xuất " + DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss");
                Annotation.Attributes["filename"] = "Lich10Ngay_" + DateTime.Now.ToString("ddMMyyyyhh:mm:ss") + ".xlsx";
                kq = orgService.Create(Annotation);

                return kq;
            }
        }
    }
}
