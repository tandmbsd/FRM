using System;
using System.ServiceModel.Description;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Security;
using DocumentFormat.OpenXml.Spreadsheet;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml;

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

        private Column CreateColumnData(UInt32 StartColumnIndex, UInt32 EndColumnIndex, double ColumnWidth)
        {
            Column column;
            column = new Column();
            column.Min = StartColumnIndex;
            column.Max = EndColumnIndex;
            column.Width = ColumnWidth;
            column.CustomWidth = true;
            return column;
        }
        private void AppendTextCell(string cellReference, string cellStringValue, Row excelRow, string bold)
        {
            //  Add a new Excel Cell to our Row 
            Cell cell = new Cell() { CellReference = cellReference, DataType = CellValues.String };
            //cell.DataType = new Bold();
            CellValue cellValue = new CellValue(bold);
            cellValue.Text = cellStringValue;
            cell.Append(cellValue);
            excelRow.Append(cell);
        }
        private void AppendTextCell(string cellReference, string cellStringValue, Row excelRow, string bold, UInt32 valindex)
        {
            //  Add a new Excel Cell to our Row 
            Cell cell = new Cell()
            {
                CellReference = cellReference,
                //StyleIndex = valindex,
                DataType = CellValues.InlineString,
                InlineString = new InlineString()
                {
                    Text = new Text()
                    {
                        Text = cellStringValue
                    }
                },
                StyleIndex = (UInt32Value)2U
            };
            //cell.DataType = new Bold();
            // CellValue cellValue = new CellValue(bold);

            //cellValue.Text = cellStringValue;
            //  cell.Append(cellValue);
            excelRow.Append(cell);
        }

        private void AppendNumericCell(string cellReference, string cellStringValue, Row excelRow, string bold, UInt32 valstyle)
        {
            //  Add a new Excel Cell to our Row 
            Cell cell = new Cell() { CellReference = cellReference, StyleIndex = valstyle, DataType = CellValues.Number };
            CellValue cellValue = new CellValue(bold);
            cellValue.Text = cellStringValue;
            cell.Append(cellValue);
            excelRow.Append(cell);
        }
        private void AppendNumericCell(string cellReference, string cellStringValue, Row excelRow, UInt32 valstyle)
        {
            //  Add a new Excel Cell to our Row 
            Cell cell = new Cell() { CellReference = cellReference, StyleIndex = valstyle, DataType = CellValues.Number };

            CellValue cellValue = new CellValue();
            cellValue.Text = cellStringValue;
            cell.Append(cellValue);
            excelRow.Append(cell);
        }
        private void AppendNumericCell(string cellReference, string cellStringValue, Row excelRow, string bold)
        {
            //  Add a new Excel Cell to our Row 
            Cell cell = new Cell() { CellReference = cellReference };
            CellValue cellValue = new CellValue(bold);
            cellValue.Text = cellStringValue;
            cell.Append(cellValue);
            excelRow.Append(cell);
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

            #endregion

            MemoryStream memoryStream = new MemoryStream();

            // --------------------------------------------SHEET 1 ----------------------------------------------------
            #region
            Entity vt = orgService.Retrieve("new_vuthuhoach", vth, new ColumnSet(new string[] { "new_name" }));
            string tieude = vt.Attributes["new_name"].ToString();

            string[] alphabet = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH" };


            using (SpreadsheetDocument myDoc = SpreadsheetDocument.Create(memoryStream, SpreadsheetDocumentType.Workbook))
            {

                WorkbookPart workbookpart = myDoc.AddWorkbookPart();
                workbookpart.Workbook = new Workbook();
                // Add a WorksheetPart to the WorkbookPart.
                WorksheetPart worksheetPart = workbookpart.AddNewPart<WorksheetPart>();
                worksheetPart.Worksheet = new Worksheet();
                WorkbookStylesPart stylesPart = myDoc.WorkbookPart.AddNewPart<WorkbookStylesPart>();
                DocumentFormat.OpenXml.Drawing.Charts.NumberingFormat num = new DocumentFormat.OpenXml.Drawing.Charts.NumberingFormat();
                stylesPart.Stylesheet = GenerateStyleSheet();
                stylesPart.Stylesheet.Save();
                Columns columns = new Columns();
                columns.Append(CreateColumnData(1, 1, 17));
                columns.Append(CreateColumnData(2, 2, 19));
                uint leng = (UInt32)(list1.Count + 1);
                columns.Append(CreateColumnData(3, leng, 11));

                worksheetPart.Worksheet.Append(columns);
                SheetData sheetData = new SheetData();
                //add a row
                Row firstRow = new Row();
                firstRow.RowIndex = (UInt32)1;
                string value = "TỔNG HỢP LỊCH THU HOẠCH VỤ ÉP " + tieude.ToUpper().ToString();
                AppendTextCell("A1", value, firstRow, "Bold", 2);
                sheetData.AppendChild(firstRow);


                string sHeaderText = "USL mía \n nguyên liệu";
                // row 2
                firstRow = new Row();
                firstRow.RowIndex = (UInt32)2;

                AppendTextCell("A2", "Trạm", firstRow, "", 2);
                AppendTextCell("B2", sHeaderText, firstRow, "", 2);
                AppendTextCell("C2", "Lịch thu hoạch đến cuối vụ", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                string ps_me = alphabet[list1.Count + 1];
                worksheetPart.Worksheet.Append(sheetData);

                //create a MergeCells class to hold each MergeCell
                MergeCells mergeCells = new MergeCells();

                // append a MergeCell to the mergeCells for each set of merged cells
                mergeCells.Append(new MergeCell() { Reference = new StringValue("A2:A4") });
                mergeCells.Append(new MergeCell() { Reference = new StringValue("B2:B4") });
                mergeCells.Append(new MergeCell() { Reference = new StringValue("C2:" + ps_me + "2") });
                mergeCells.Append(new MergeCell() { Reference = new StringValue("A1:" + alphabet[list1.Count + 1] + "1") });

                // gen đợt thu hoạch

                Row row3 = new Row();
                Row row4 = new Row();
                row3.RowIndex = (UInt32)3;
                row4.RowIndex = (UInt32)4;
                for (int j = 0; j < list1.Count; j++)
                {
                    // tạo đợt

                    string ps_cel = alphabet[j + 2];
                    AppendTextCell(ps_cel + "3", list1.Values.ToList()[j], row3, "", 2);
                    // tạo ngày 
                    foreach (Entity a in entc1.Entities)
                    {
                        if (a["new_name"].ToString() == list1.Values.ToList()[j])
                        {
                            DateTime tungay_full = ((DateTime)a["new_tungay"]).AddHours(7);
                            DateTime denngay_full = ((DateTime)a["new_denngay"]).AddHours(7);
                            int tungay_ngay = tungay_full.Day;
                            int denngay_ngay = denngay_full.Day;
                            int tungay_thang = tungay_full.Month;
                            int denngay_thang = denngay_full.Month;
                            AppendTextCell(ps_cel + "4", "(" + tungay_ngay + "/" + tungay_thang + "-" + "\n" + denngay_ngay + "/" + denngay_thang + ")", row4, "", 2);
                        }
                    }
                }
                sheetData.AppendChild(row3);
                sheetData.AppendChild(row4);
                // tạo nội dung
                int ps = 5;
                int sum_all = 0;
                int o = 5;
                foreach (var i in list_tram)
                {
                    Row row_tram = new Row();
                    row_tram.RowIndex = (UInt32)(o);
                    int sum_tram = 0;
                    int sum_tram_test = 0;
                    AppendTextCell("A" + ps, i, row_tram, "", 1);
                    for (int t = 0; t < entc.Entities.Count; t++)
                    {
                        Guid id_tram = ((EntityReference)((AliasedValue)entc.Entities[t].Attributes["new_tram"]).Value).Id;
                        string tram = lst_tram.FirstOrDefault(x => x.Key == id_tram).Value;
                        Guid id_dot = ((EntityReference)((AliasedValue)entc.Entities[t].Attributes["new_dotthuhoach"]).Value).Id;
                        string dotthuhoach = list1.FirstOrDefault(x => x.Key == id_dot).Value;
                        if (i == tram)
                        {
                            sum_tram_test += Int32.Parse((((AliasedValue)entc.Entities[t].Attributes["SUM_new_sanluong"]).Value).ToString());
                        }
                    }
                    AppendNumericCell("B" + ps, sum_tram_test.ToString(), row_tram, "", 1);

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
                                    string vl_cell = ((((AliasedValue)entc.Entities[j].Attributes["SUM_new_sanluong"]).Value).ToString());
                                    string clom = alphabet[k + 2];
                                    AppendNumericCell(clom + (ps), vl_cell, row_tram, "", 3);
                                }
                            }
                            sum_tram += Int32.Parse((((AliasedValue)entc.Entities[j].Attributes["SUM_new_sanluong"]).Value).ToString());
                        }
                    }
                    sum_all += sum_tram;
                    sheetData.AppendChild(row_tram);
                    ps++;
                    o++;

                    Console.WriteLine(i);
                }
                // row tổng sl
                Row row_tongsl = new Row();
                row_tongsl.RowIndex = (UInt32)(list_tram.Count + 5);
                AppendTextCell("A" + (list_tram.Count + 5), "Tổng SL", row_tongsl, "", 1);
                AppendNumericCell("B" + (list_tram.Count + 5), sum_all.ToString(), row_tongsl, 1);
                for (var i = 0; i < sum_dotthuhoach.Count; i++)
                {
                    string ps_dot = alphabet[i + 2];
                    AppendNumericCell(ps_dot + (list_tram.Count + 5), sum_dotthuhoach[i].ToString(), row_tongsl, "", 1);
                }
                sheetData.AppendChild(row_tongsl);
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
                            //worksheet.Cells[list_tram.Count + 5, ps_hd] = new Cell(list_ngayhd.Values.ToList()[k]);
                            tongngayhd += list_ngayhd.Values.ToList()[k];
                            // worksheet.Cells[list_tram.Count + 6, ps_hd] = new Cell(sum_dotthuhoach[j] / list_ngayhd.Values.ToList()[k], "#,##");
                        }
                    }
                    ps_hd++;
                }
                // row Số ngày hoạt động và bq/ngày
                Row row_songayhd = new Row();
                Row row_bqngay = new Row();
                row_songayhd.RowIndex = (UInt32)(list_tram.Count + 6);
                row_bqngay.RowIndex = (UInt32)(list_tram.Count + 7);
                AppendNumericCell("A" + (list_tram.Count + 6), "Số ngày hoạt động", row_songayhd, "");
                AppendNumericCell("A" + (list_tram.Count + 7), "SL BQ/ngày", row_bqngay, "", 4);
                AppendNumericCell("B" + (list_tram.Count + 6), tongngayhd.ToString(), row_songayhd, "");
                AppendNumericCell("B" + (list_tram.Count + 7), (sum_all / tongngayhd).ToString(), row_bqngay, "", 4);


                int ps_hd2 = 2;
                for (int j = 0; j < list1.Count; j++)
                {
                    // tạo số ngày hoạt động
                    for (int k = 0; k < list_ngayhd.Count; k++)
                    {
                        if (list_ngayhd.Keys.ToList()[k] == list1.Values.ToList()[j])
                        {
                            string ps_hd_al = alphabet[ps_hd2];
                            AppendNumericCell(ps_hd_al + (list_tram.Count + 6), (list_ngayhd.Values.ToList()[k]).ToString(), row_songayhd, "");
                            AppendNumericCell(ps_hd_al + (list_tram.Count + 7), (sum_dotthuhoach[j] / list_ngayhd.Values.ToList()[k]).ToString(), row_bqngay, "", 4);

                        }
                    }
                    ps_hd2++;
                }

                sheetData.AppendChild(row_songayhd);
                sheetData.AppendChild(row_bqngay);
                worksheetPart.Worksheet.InsertAfter(mergeCells, worksheetPart.Worksheet.Elements<SheetData>().First());

                //this is the part that was missing from your code
                Sheets sheet1 = myDoc.WorkbookPart.Workbook.AppendChild(new Sheets());
                sheet1.AppendChild(new Sheet()
                {
                    Id = myDoc.WorkbookPart.GetIdOfPart(myDoc.WorkbookPart.WorksheetParts.First()),
                    SheetId = 1,
                    Name = "Sheet1"
                });
            }
            #endregion
            ////--------------------------------------SHEEET 2 --------------------------------------------------------
            #region
            using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Open(memoryStream, true))
            {
                // Add a blank WorksheetPart.
                WorksheetPart newWorksheetPart = spreadSheet.WorkbookPart.AddNewPart<WorksheetPart>();
                newWorksheetPart.Worksheet = new Worksheet();
                Sheets sheets = spreadSheet.WorkbookPart.Workbook.GetFirstChild<Sheets>();
                string relationshipId = spreadSheet.WorkbookPart.GetIdOfPart(newWorksheetPart);

                Worksheet worksheet = new Worksheet();
                worksheet.AddNamespaceDeclaration("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships");

                // I don't know what SheetDimension.Reference is used for, it doesn't seem to change the resulting xml.
                SheetDimension sheetDimension = new SheetDimension() { Reference = "A1:A6" };
                SheetViews sheetViews = new SheetViews();
                // If more than one SheetView.TabSelected is set to true, it looks like Excel just picks the first one.
                SheetView sheetView = new SheetView() { TabSelected = false, WorkbookViewId = 0U };
                Pane pane = new Pane()
                {
                    VerticalSplit = 5D,
                    TopLeftCell = "A6",
                    ActivePane = PaneValues.BottomLeft,
                    State = PaneStateValues.Frozen
                };
                // I don't know what Selection.ActiveCell is used for, it doesn't seem to change the resulting xml.
                DocumentFormat.OpenXml.Spreadsheet.Selection selection = new DocumentFormat.OpenXml.Spreadsheet.Selection() { ActiveCell = "A5", SequenceOfReferences = new ListValue<StringValue>() { InnerText = "A5" } };
                sheetView.Append(pane);
                sheetView.Append(selection);
                sheetViews.Append(sheetView);
                SheetFormatProperties sheetFormatProperties = new SheetFormatProperties() { DefaultRowHeight = 15D };
                DocumentFormat.OpenXml.Spreadsheet.PageMargins pageMargins = new DocumentFormat.OpenXml.Spreadsheet.PageMargins() { Left = 0.7D, Right = 0.7D, Top = 0.7D, Bottom = 0.75D, Header = 0.3D, Footer = 0.3D };



                // chỉnh độ rộng của các cột
                Columns columns = new Columns();
                uint leng = (UInt32)(list1.Count + 11);
                columns.Append(CreateColumnData(1, 1, 10));
                columns.Append(CreateColumnData(2, 2, 22));
                columns.Append(CreateColumnData(3, 3, 10));
                columns.Append(CreateColumnData(4, 4, 24));
                columns.Append(CreateColumnData(5, 7, 7));
                columns.Append(CreateColumnData(8, 8, 10));
                columns.Append(CreateColumnData(9, 9, 12));
                columns.Append(CreateColumnData(10, 10, 9));
                columns.Append(CreateColumnData(11, 11, 11));
                columns.Append(CreateColumnData(12, 12, 18));
                columns.Append(CreateColumnData(13, leng, 12));
                columns.Append(CreateColumnData(leng + 2, leng + 2, 8));
                columns.Append(CreateColumnData(leng + 3, leng + 3, 27));


                SheetData sheetData = new SheetData();
                #region fetch
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
                #endregion
                #region interface
                Entity vt2 = orgService.Retrieve("new_vuthuhoach", vth, new ColumnSet(new string[] { "new_name" }));

                string tieude2 = vt.Attributes["new_name"].ToString();
                Row firstRow = new Row();
                firstRow.RowIndex = (UInt32)1;
                string value = "CHI TIẾT LỊCH " + tieude.ToUpper() + "  ĐẾN CUỐI VỤ";
                AppendTextCell("A1", value, firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)3;
                AppendTextCell("M3", "Lịch thu hoạch đến cuối vụ", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                // row đợt thu hoạch
                firstRow = new Row();
                firstRow.RowIndex = (UInt32)4;
                for (int i = 0; i < list1.Count; i++)
                {
                    AppendTextCell(alphabet[i + 12] + 4, list1.Values.ToList()[i], firstRow, "", 2);

                }
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("A5", "Trạm", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("B5", "CBNV", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("C5", "CMND", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("D5", "Họ tên", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("E5", "Số HĐ", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("F5", "Mã vùng", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("G5", "Số thửa", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("H5", "Giống mía", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("I5", "Diện tích", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("J5", "Loại mía", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("K5", "Ngày trồng", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                firstRow = new Row();
                firstRow.RowIndex = (UInt32)5;
                AppendTextCell("L5", "SL mía \n nguyên \n liệu", firstRow, "", 2);
                sheetData.AppendChild(firstRow);

                #endregion
                // gen ngày
                Row row_ngay = new Row();
                row_ngay.RowIndex = (UInt32)5;
                for (int i = 0; i < list1.Count; i++)
                {
                    //AppendTextCell(alphabet[i + 12] + 4, list1.Values.ToList()[i],firstRow, "");
                    // tạo ngày 
                    foreach (Entity a in entc1.Entities)
                    {
                        if (a["new_name"].ToString() == list1.Values.ToList()[i])
                        {
                            DateTime tungay_full = ((DateTime)a["new_tungay"]).AddHours(7);
                            DateTime denngay_full = ((DateTime)a["new_denngay"]).AddHours(7);
                            int tungay_ngay = tungay_full.Day;
                            int denngay_ngay = denngay_full.Day;
                            int tungay_thang = tungay_full.Month;
                            int denngay_thang = denngay_full.Month;
                            AppendTextCell(alphabet[i + 12] + 5, "(" + tungay_ngay + "/" + tungay_thang + "-" + "\n" + denngay_ngay + "/" + denngay_thang + ")", row_ngay, "", 2);
                        }
                    }
                }
                //tuổi mía và nguyên nhân thu hoạch sớm
                AppendTextCell(alphabet[list1.Count + 12] + 5, "Tuổi mía", firstRow, "", 2);
                AppendTextCell(alphabet[list1.Count + 13] + 5, "Nguyên nhân \n thu hoạch \n sớm", firstRow, "", 2);

                sheetData.AppendChild(row_ngay);
                // tính tổng theo đợt và tổng theo trạm thửa
                Dictionary<string, int> sum_dot_sheet2 = new Dictionary<string, int>();
                // sum theo thửa đất
                Dictionary<string, int> sum_thuadat_sheet2 = new Dictionary<string, int>();
                // list thửa đất canh tác
                Dictionary<string, int> list_thuadat = new Dictionary<string, int>();


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

                        firstRow = new Row();
                        firstRow.RowIndex = (UInt32)ps_row;
                        if (!list_thuadat.ContainsKey(check))
                        {


                            // trạm
                            AppendTextCell("A" + ps_row, ((EntityReference)en.Attributes["new_tram"]).Name, firstRow, "", 2);
                            // cbnv
                            if (en.Contains("aa.new_canbonongvu"))
                            {
                                AppendTextCell("B" + ps_row, ((EntityReference)((AliasedValue)en.Attributes["aa.new_canbonongvu"]).Value).Name, firstRow, "");
                            }
                            if (en.Contains("ad.new_masothue"))
                            {
                                AppendTextCell("C" + ps_row, ((AliasedValue)(en.Attributes["ad.new_masothue"])).Value.ToString(), firstRow, "");
                            }
                            else
                            {
                                AppendTextCell("C" + ps_row, ((AliasedValue)(en.Attributes["ab.new_socmnd"])).Value.ToString(), firstRow, "");
                            }
                            // ho ten
                            if (en.Contains("aa.new_khachhang"))
                            {
                                AppendTextCell("D" + ps_row, ((EntityReference)((AliasedValue)en.Attributes["aa.new_khachhang"]).Value).Name, firstRow, "");
                            }
                            else
                                AppendTextCell("D" + ps_row, ((EntityReference)((AliasedValue)en.Attributes["aa.new_khachhangdoanhnghiep"]).Value).Name, firstRow, "");


                            // số HĐ
                            string mahd = ((AliasedValue)(en.Attributes["ac.new_masohopdong"])).Value.ToString();
                            AppendTextCell("E" + ps_row, mahd.Substring(4, 6).ToString(), firstRow, "");
                            // mã vùng
                            string sl = ((EntityReference)((AliasedValue)en.Attributes["aa.new_thuadat"]).Value).Name;
                            AppendTextCell("F" + ps_row, sl.Substring(0, 5).ToString(), firstRow, "");
                            //  số thửa
                            AppendTextCell("G" + ps_row, sl.Substring(6, 4).ToString(), firstRow, "");
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
                                AppendTextCell("H" + ps_row, tengiongmia, firstRow, "");
                            }

                            // diện tích
                            AppendTextCell("I" + ps_row, ((AliasedValue)en.Attributes["aa.new_dientichconlai"]).Value.ToString(), firstRow, "");
                            //loại mía
                            AppendTextCell("J" + ps_row, ((EntityReference)((AliasedValue)en.Attributes["aa.new_giongtrongthucte"]).Value).Name, firstRow, "");
                            // ngày trồng
                            if (en.Contains("aa.new_ngaytrong"))
                            {
                                DateTime date = (DateTime)((AliasedValue)en.Attributes["aa.new_ngaytrong"]).Value;
                                AppendTextCell("K" + ps_row, date.ToString("dd/MM/yyyy"), firstRow, "");
                            }

                            // số lượng mía nguyên liệu
                            list_thuadat.Add(check, ps_row);
                            AppendNumericCell("L" + ps_row, "0", firstRow, "", 2);
                            // add ô sản lượng
                            int ps_record = 0;
                            for (ps_record = 0; ps_record < list1.Count; ps_record++)
                            {
                                if (list1.Values.ToList()[ps_record] == ((EntityReference)en.Attributes["new_dotthuhoach"]).Name)
                                {
                                    AppendNumericCell(alphabet[ps_record + 12] + ps_row, en.Attributes["new_sanluong"].ToString(), firstRow, "");
                                }
                                else
                                    AppendTextCell(alphabet[ps_record + 12] + ps_row, "", firstRow, "");
                            }

                            // tuổi  mía và nguyên nhân

                            if (en.Contains("new_tuoimia"))
                            {
                                float tuoimia = float.Parse(en.Attributes["new_tuoimia"].ToString());
                                //int phannguyen = (int)tuoimia / 1;
                                //float phandu = (int)((((float)tuoimia % 1) * 30) / 1);
                                //Math.Round(tuoimia, 1)
                                AppendNumericCell(alphabet[list1.Count + 12] + ps_row, Math.Round(tuoimia, 1).ToString(), firstRow, "");
                            }

                            // nguyên nhân thu hoạch sớm

                            if (en.Contains("new_lydothuhoachsom"))
                            {
                                AppendTextCell(alphabet[list1.Count + 13] + ps_row, ((EntityReference)en.Attributes["new_lydothuhoachsom"]).Name, firstRow, "");
                            }
                            Console.WriteLine("insert row: " + (ps_row - 6));
                            ps_row++;
                            sheetData.AppendChild(firstRow);
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

                            var theRow = sheetData.Elements<Row>().Where(r => r.RowIndex.Value == keysWithMatchingValues).FirstOrDefault();
                            string val = alphabet[ps_record + 12] + keysWithMatchingValues;
                            Cell refCell = theRow.Elements<Cell>().Where(c => c.CellReference == val).FirstOrDefault();
                            string cellValue = refCell.CellValue.InnerXml.ToString();

                            if (cellValue == "")
                            {
                                //  int cell_value = Int32.Parse(cellValue);
                                Console.WriteLine("--------------" + en.Attributes["new_sanluong"].ToString());
                                refCell.CellValue = new CellValue(en.Attributes["new_sanluong"].ToString());
                                refCell.DataType = new EnumValue<CellValues>(CellValues.Number);
                            }
                            else
                            {
                                int cellValue_int = Int32.Parse(cellValue.ToString()) + Int32.Parse(en.Attributes["new_sanluong"].ToString());
                                refCell.CellValue = new CellValue(cellValue_int.ToString());
                                refCell.DataType = new EnumValue<CellValues>(CellValues.Number);
                            }


                            Console.WriteLine("test: " + p);
                            p++;
                        }


                    }

                    #endregion

                }
                while (entc2.MoreRecords);
                // row tổng cộng
                firstRow = new Row();
                firstRow.RowIndex = (UInt32)(ps_row);
                AppendTextCell("A" + (ps_row), "Tổng cộng", firstRow, "", 2);

                for (int i = 0; i < list1.Count; i++)
                {
                    for (int j = 0; j < sum_dot_sheet2.Count; j++)
                    {
                        if (list1.Values.ToList()[i] == sum_dot_sheet2.Keys.ToList()[j])
                            AppendNumericCell(alphabet[i + 12] + (ps_row), sum_dot_sheet2.Values.ToList()[j].ToString(), firstRow, "", 1);
                    }
                }
                sheetData.AppendChild(firstRow);
                // sum theo thửa đất

                for (int i = 0; i < sum_thuadat_sheet2.Count; i++)
                {
                    firstRow = new Row();
                    firstRow.RowIndex = (UInt32)(i + 6);
                    var theRow = sheetData.Elements<Row>().Where(r => r.RowIndex.Value == (i + 6)).FirstOrDefault();
                    string val = "L" + (i + 6);
                    Cell refCell = theRow.Elements<Cell>().Where(c => c.CellReference == val).FirstOrDefault();
                    string cellValue = refCell.CellValue.InnerXml.ToString();
                    refCell.CellValue = new CellValue(sum_thuadat_sheet2.Values.ToList()[i].ToString());
                    refCell.DataType = new EnumValue<CellValues>(CellValues.Number);

                }
                //worksheet.Append(sheetDimension);

                worksheet.Append(sheetViews);
                worksheet.AppendChild(columns);
                //worksheet.Append(sheetFormatProperties);
                worksheet.Append(sheetData);
                // worksheet.Append(pageMargins);


                AutoFilter autoFilter1 = new AutoFilter()
                { Reference = "A5:" + alphabet[list1.Count + 13] + ps_row };

                newWorksheetPart.Worksheet = worksheet;
                MergeCells mergeCells = new MergeCells();
                // append a MergeCell to the mergeCells for each set of merged cells
                mergeCells.Append(new MergeCell() { Reference = new StringValue("M3:Y3") });
                // mergeCells.Append(new MergeCell() { Reference = new StringValue("A"+ps_row+":L" +ps_row) });
                mergeCells.Append(new MergeCell() { Reference = new StringValue("A1:" + alphabet[14 + list1.Count] + "1") });


                newWorksheetPart.Worksheet.InsertAfter(mergeCells, newWorksheetPart.Worksheet.Elements<SheetData>().First());
                newWorksheetPart.Worksheet.InsertAfter(autoFilter1, newWorksheetPart.Worksheet.Elements<SheetData>().First());
                //newWorksheetPart.Worksheet.InsertAfter(columns, newWorksheetPart.Worksheet.Elements<SheetData>().First());


                // Get a unique ID for the new worksheet.
                uint sheetId = 2;
                if (sheets.Elements<Sheet>().Count() > 0)
                {
                    sheetId = sheets.Elements<Sheet>().Select(s => s.SheetId.Value).Max() + 1;
                }
                // Give the new worksheet a name.
                string sheetName = "Sheet2";

                // Append the new worksheet and associate it with the workbook.
                Sheet sheet = new Sheet() { Id = relationshipId, SheetId = sheetId, Name = sheetName };
                sheets.Append(sheet);
            }


            #endregion

            memoryStream.Seek(0, SeekOrigin.Begin);

            Guid kq = Guid.Empty;
            Entity Annotation = new Entity("annotation");
            Annotation.Attributes["objectid"] = new EntityReference("new_vuthuhoach", vth);
            Annotation.Attributes["objecttypecode"] = "new_vuthuhoach";
            Annotation.Attributes["subject"] = "Lịch thu hoạch 10 ngày " + DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss");
            Annotation.Attributes["documentbody"] = Convert.ToBase64String(memoryStream.ToArray());
            Annotation.Attributes["mimetype"] = @"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            Annotation.Attributes["notetext"] = "File xuất excel lịch thu hoạch 10 ngày. Ngày xuất " + DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss");
            Annotation.Attributes["filename"] = "Lich10Ngay_" + DateTime.Now.ToString("ddMMyyyyhh:mm:ss") + ".xls";
            kq = orgService.Create(Annotation);

            return kq;

        }
        private Worksheet FreezeRow(WorksheetPart worksheetPart)
        {
            return worksheetPart.Worksheet;
        }
        private Cell GetCell(Worksheet worksheet, string columnName, uint rowIndex)
        {
            Row row = GetRow(worksheet, rowIndex);

            if (row == null)
                return null;

            return row.Elements<Cell>().Where(c => string.Compare
                   (c.CellReference.Value, columnName +
                   rowIndex, true) == 0).First();
        }
        private WorksheetPart GetWorksheetPart(WorkbookPart workbookPart, string v)
        {
            string relId = workbookPart.Workbook.Descendants<Sheet>().First(s => v.Equals(s.Name)).Id;
            return (WorksheetPart)workbookPart.GetPartById(relId);
        }

        private Row GetRow(Worksheet worksheet, uint rowIndex)
        {
            return worksheet.GetFirstChild<SheetData>().
              Elements<Row>().Where(r => r.RowIndex == rowIndex).First();
        }
        private Stylesheet GenerateStyleSheet()
        {
            DocumentFormat.OpenXml.Drawing.Charts.NumberingFormat num = new DocumentFormat.OpenXml.Drawing.Charts.NumberingFormat();
            return new Stylesheet(
                new Fonts(
                    new Font(                                                               // Index 0 – The default font.
                        new FontSize() { Val = 11 },
                        new Color() { Rgb = new HexBinaryValue() { Value = "000000" } },
                        new FontName() { Val = "Calibri" }),
                    new Font(                                                               // Index 1 – The bold font.
                        new Bold(),
                        new FontSize() { Val = 11 },
                        new Color() { Rgb = new HexBinaryValue() { Value = "000000" } },
                        new FontName() { Val = "Calibri" }),
                    new Font(                                                               // Index 2 – The Italic font.

                        new Italic(),
                        new FontSize() { Val = 11 },
                        new Color() { Rgb = new HexBinaryValue() { Value = "000000" } },
                        new FontName() { Val = "Calibri" }),
                    new Font(                                                               // Index 2 – The Times Roman font. with 16 size
                        new FontSize() { Val = 11 },
                        new Color() { Rgb = new HexBinaryValue() { Value = "000000" } },
                        new FontName() { Val = "Calibri" })
                ),
                new Fills(
                    new Fill(                                                           // Index 0 – The default fill.
                        new PatternFill() { PatternType = PatternValues.None }),
                    new Fill(                                                           // Index 1 – The default fill of gray 125 (required)
                        new PatternFill() { PatternType = PatternValues.Gray125 }),
                    new Fill(                                                           // Index 2 – The yellow fill.
                        new PatternFill(
                            new ForegroundColor() { Rgb = new HexBinaryValue() { Value = "FFFFFF00" } }
                        )
                        { PatternType = PatternValues.Solid })
                ),
                new Borders(
                    new Border(                                                         // Index 0 – The default border.
                        new LeftBorder(),
                        new RightBorder(),
                        new TopBorder(),
                        new BottomBorder(),
                        new DiagonalBorder()),
                    new Border(                                                         // Index 1 – Applies a Left, Right, Top, Bottom border to a cell
                        new LeftBorder(
                            new Color() { Auto = true }
                        )
                        { Style = BorderStyleValues.Thin },
                        new RightBorder(
                            new Color() { Auto = true }
                        )
                        { Style = BorderStyleValues.Thin },
                        new TopBorder(
                            new Color() { Auto = true }
                        )
                        { Style = BorderStyleValues.Thin },
                        new BottomBorder(
                            new Color() { Auto = true }
                        )
                        { Style = BorderStyleValues.Thin },
                        new DiagonalBorder())
                ),
                new CellFormats(
                    new CellFormat() { FontId = 0, FillId = 0, BorderId = 0, NumberFormatId = 3 },                          // Index 0 – The default cell style.  If a cell does not have a style index applied it will use this style combination instead
                    new CellFormat() { FontId = 1, FillId = 0, BorderId = 0, ApplyFont = true, NumberFormatId = 3 },       // Index 1 – Bold 
                    new CellFormat(new Alignment() { Horizontal = HorizontalAlignmentValues.Center, Vertical = VerticalAlignmentValues.Center, WrapText = true }) { FontId = 1, FillId = 0, BorderId = 0, ApplyFont = true, NumberFormatId = 3 },       // Index 2 – Italic
                    new CellFormat() { FontId = 3, FillId = 0, BorderId = 0, ApplyFont = true, NumberFormatId = 3 },       // Index 3 – Times Roman
                    new CellFormat() { FontId = 2, FillId = 0, BorderId = 0, ApplyFill = true, NumberFormatId = 3 },       // Index 4 – Yellow Fill

                    new CellFormat(                                                                   // Index 5 – Alignment
                        new Alignment() { Horizontal = HorizontalAlignmentValues.Center, Vertical = VerticalAlignmentValues.Center }
                    )
                    { FontId = 0, FillId = 0, BorderId = 0, ApplyAlignment = true },
                    new CellFormat() { FontId = 0, FillId = 0, BorderId = 1, ApplyBorder = true },  // Index 6 – Border
                    new CellFormat() { NumberFormatId = 3, ApplyNumberFormat = true },
                    new CellFormats() { Count = (UInt32Value)2U },
                    new CellFormat() { NumberFormatId = (UInt32Value)0U, FontId = (UInt32Value)0U, FillId = (UInt32Value)0U, BorderId = (UInt32Value)0U, FormatId = (UInt32Value)0U }

                    )

            ); // return
        }
    }
}
