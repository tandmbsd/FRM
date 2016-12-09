using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Sdk.XmlNamespaces;
using Microsoft.Xrm.Client.Services;
using System.Configuration;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.WebServiceClient;
using System.ServiceModel;
using System.ServiceModel.Security;

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            OrganizationService service;
            var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            var serverConnection = new ServerConnection(connectionstring);

            using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                Guid entityId = new Guid("2D81712C-4EB7-E511-93F1-9ABE942A7E29");//{472AA958-BFB5-E511-93F1-9ABE942A7E29} 24EB7CBB-B5B5-E511-93F1-9ABE942A7E29 

                Entity CSTM = service.Retrieve("new_chinhsachthumua", entityId, new ColumnSet(true));

                Entity newCSTM = new Entity("new_chinhsachthumua");

                //Thông tin chính sách
                EntityReference vudautuRef = CSTM.GetAttributeValue<EntityReference>("new_vudautu");
                Guid vuDTId = vudautuRef.Id;
                Entity vuDT = service.Retrieve("new_vudautu", vuDTId, new ColumnSet(new string[] { "new_name", "new_ngaybatdau" }));

                Entity newvuDT = FindvuDT(service, vuDT);
                if (newvuDT != null && newvuDT.Id != Guid.Empty)
                {
                    newCSTM["new_vudautu"] = newvuDT.ToEntityReference();

                    int hdapdung = ((OptionSetValue)CSTM["new_hoatdongapdung"]).Value;
                    newCSTM["new_hoatdongapdung"] = new OptionSetValue(hdapdung);

                    string newName = CSTM["new_name"].ToString() + " vụ " + newvuDT["new_name"].ToString();
                    newCSTM["new_name"] = newName;

                    DateTime thoidiemapdung = DateTime.Now;
                    newCSTM["new_thoidiemapdung"] = thoidiemapdung;

                    // Chi tiết
                    if (CSTM.Attributes.Contains("new_vutrong_vl"))   // vu trong
                    {
                        string vutrong = CSTM["new_vutrong_vl"].ToString();
                        newCSTM.Attributes["new_vutrong_vl"] = vutrong;
                    }
                    if (CSTM.Attributes.Contains("new_nhomdat_vl"))   // nhom dat
                    {
                        string nhomdat = CSTM["new_nhomdat_vl"].ToString();
                        newCSTM.Attributes["new_nhomdat_vl"] = nhomdat;
                    }
                    if (CSTM.Attributes.Contains("new_loaigocmia_vl"))   // loai goc mia
                    {
                        string loaigocmia = CSTM["new_loaigocmia_vl"].ToString();
                        newCSTM.Attributes["new_loaigocmia_vl"] = loaigocmia;
                    }
                    if (CSTM.Attributes.Contains("new_nhomgiongmia_vl"))   // nhom giong mia
                    {
                        string nhomgiongmia = CSTM["new_nhomgiongmia_vl"].ToString();
                        newCSTM.Attributes["new_nhomgiongmia_vl"] = nhomgiongmia;
                    }

                    if (CSTM.Attributes.Contains("new_tinhtrangmia_vl"))   // tinh trang mia
                    {
                        string tinhtrangmia = CSTM["new_tinhtrangmia_vl"].ToString();
                        newCSTM.Attributes["new_tinhtrangmia_vl"] = tinhtrangmia;
                    }
                    if (CSTM.Attributes.Contains("new_loaimiachay_vl"))   // loai mia chay
                    {
                        string loaimiachay = CSTM["new_loaimiachay_vl"].ToString();
                        newCSTM.Attributes["new_loaimiachay_vl"] = loaimiachay;
                    }

                    if (CSTM.Attributes.Contains("new_loaisohuudat_vl"))   // loai so huu dat
                    {
                        string loaishdat = CSTM["new_loaisohuudat_vl"].ToString();
                        newCSTM.Attributes["new_loaisohuudat_vl"] = loaishdat;
                    }

                    newCSTM["new_miadonga"] = (bool)CSTM["new_miadonga"];     // mía đổ ngã

                    if (CSTM.Attributes.Contains("new_mucdichsanxuatmia_vl"))   // muc dich sx mia
                    {
                        string mucdichsxmia = CSTM["new_mucdichsanxuatmia_vl"].ToString();
                        newCSTM.Attributes["new_mucdichsanxuatmia_vl"] = mucdichsxmia;
                    }

                    if (CSTM.Attributes.Contains("new_phuongphapthuhoach_vl"))   // pp thu hoạch
                    {
                        string ppthuhoach = CSTM["new_phuongphapthuhoach_vl"].ToString();
                        newCSTM.Attributes["new_phuongphapthuhoach_vl"] = ppthuhoach;
                    }
                    if (CSTM.Attributes.Contains("new_loaikhoiluong_vl"))   // loai khối lượng
                    {
                        string loaikl = CSTM["new_loaikhoiluong_vl"].ToString();
                        newCSTM.Attributes["new_loaikhoiluong_vl"] = loaikl;
                    }

                    newCSTM["new_congdieutunoikhac"] = (bool)CSTM["new_congdieutunoikhac"];     // công điều từ nơi khác

                    // Đơn giá CCS
                    if (CSTM.Attributes.Contains("new_dongiamiacobantainhamay"))   // đơn giá cơ bản 10 ccs nhà máy
                    {
                        Money dongianhamay = (Money)CSTM["new_dongiamiacobantainhamay"];
                        newCSTM["new_dongiamiacobantainhamay"] = dongianhamay;
                    }
                    if (CSTM.Attributes.Contains("new_dongiamiacobantairuong"))   // đơn giá cơ bản 10 ccs ruộng
                    {
                        Money dongiaruong = (Money)CSTM["new_dongiamiacobantairuong"];
                        newCSTM["new_dongiamiacobantairuong"] = dongiaruong;
                    }
                    if (CSTM.Attributes.Contains("new_dongiatang1ccs"))   // đơn giá tăng 1 ccs
                    {
                        Money tang1ccs = (Money)CSTM["new_dongiatang1ccs"];
                        newCSTM["new_dongiatang1ccs"] = tang1ccs;
                    }
                    if (CSTM.Attributes.Contains("new_dongiagiam1ccs"))   // đơn giá giam 1 ccs
                    {
                        Money giam1ccs = (Money)CSTM["new_dongiagiam1ccs"];
                        newCSTM["new_dongiagiam1ccs"] = giam1ccs;
                    }

                    // Tiền mía

                    newCSTM["new_hoanthanhhopdong"] = (bool)CSTM["new_hoanthanhhopdong"];     // hoàn thành hợp đồng

                    if (CSTM.Attributes.Contains("new_thuonghoanthanhhd"))   // thưởng hoàn thành hợp đồng
                    {
                        Money tienthuongHTHD = (Money)CSTM["new_thuonghoanthanhhd"];
                        newCSTM["new_thuonghoanthanhhd"] = tienthuongHTHD;
                    }

                    if (CSTM.Attributes.Contains("new_thuongchatsatgoc"))   // thưởng chặt sát gốc
                    {
                        Money tienthuongChatSatGoc = (Money)CSTM["new_thuongchatsatgoc"];
                        newCSTM["new_thuongchatsatgoc"] = tienthuongChatSatGoc;
                    }
                    if (CSTM.Attributes.Contains("new_phantramtilemiachay"))   // phần trăm mía cháy
                    {
                        decimal phantramMiaChay = (CSTM.Contains("new_phantramtilemiachay") ? (decimal)CSTM["new_phantramtilemiachay"] : 0);
                        newCSTM["new_phantramtilemiachay"] = phantramMiaChay;
                    }
                    if (CSTM.Attributes.Contains("new_dinhmucthuongmiachay"))   // định mức thưởng  mía cháy
                    {
                        Money dmThuongMiaChay = (Money)CSTM["new_dinhmucthuongmiachay"];
                        newCSTM["new_dinhmucthuongmiachay"] = dmThuongMiaChay;
                    }

                    // Hỗ trợ  
                    if (CSTM.Attributes.Contains("new_dongiahotromuamia"))   // đơn giá hỗ trợ mua mía
                    {
                        Money dgHoTroMuamia = (Money)CSTM["new_dongiahotromuamia"];
                        newCSTM["new_dongiahotromuamia"] = dgHoTroMuamia;
                    }
                    if (CSTM.Attributes.Contains("new_dongiahotrovanchuyen"))   // đơn giá hỗ trợ vận chuyển
                    {
                        Money dgHoTroVC = (Money)CSTM["new_dongiahotrovanchuyen"];
                        newCSTM["new_dongiahotrovanchuyen"] = dgHoTroVC;
                    }
                    if (CSTM.Attributes.Contains("new_dongiahotrothuhoach"))   // đơn giá hỗ trợ thu hoạch
                    {
                        Money dgHoTroTH = (Money)CSTM["new_dongiahotrothuhoach"];
                        newCSTM["new_dongiahotrothuhoach"] = dgHoTroTH;
                    }

                    // Tạm giữ

                    if (CSTM.Attributes.Contains("new_cachtinhtientamgiu"))    // cách tính tiền tạm giữ
                    {
                        int cachtinhtienTG = ((OptionSetValue)CSTM["new_cachtinhtientamgiu"]).Value;
                        newCSTM["new_cachtinhtientamgiu"] = new OptionSetValue(cachtinhtienTG);
                    }
                    if (CSTM.Attributes.Contains("new_tamgiutienmia"))  // tạm giữ tiền mía
                    {
                        Money tamgiuTienmia = (Money)CSTM["new_tamgiutienmia"];
                        newCSTM["new_tamgiutienmia"] = tamgiuTienmia;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiutoidatienmia"))  // tạm giữ tối đa tiền mía
                    {
                        Money tamgiuTDTienmia = (Money)CSTM["new_tamgiutoidatienmia"];
                        newCSTM["new_tamgiutoidatienmia"] = tamgiuTDTienmia;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiuphantramvanchuyen"))  // tạm giữ % VC
                    {
                        decimal tamgiuPhantramVC = (decimal)CSTM["new_tamgiuphantramvanchuyen"];
                        newCSTM["new_tamgiuphantramvanchuyen"] = tamgiuPhantramVC;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiusotienvanchuyen"))  // tạm giữ số tiền VC
                    {
                        Money tamgiuSotienVC = (Money)CSTM["new_tamgiusotienvanchuyen"];
                        newCSTM["new_tamgiusotienvanchuyen"] = tamgiuSotienVC;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiutoidavanchuyen"))  // tạm giữ tối đa VC
                    {
                        Money tamgiuTDVC = (Money)CSTM["new_tamgiutoidavanchuyen"];
                        newCSTM["new_tamgiutoidavanchuyen"] = tamgiuTDVC;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiuphantramvanchuyen"))  // tạm giữ % TH
                    {
                        decimal tamgiuPhantramTH = (decimal)CSTM["new_tamgiuphantramthuhoach"];
                        newCSTM["new_tamgiuphantramthuhoach"] = tamgiuPhantramTH;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiusotienthuhoach"))  // tạm giữ số tiền TH
                    {
                        Money tamgiuSotienTH = (Money)CSTM["new_tamgiusotienthuhoach"];
                        newCSTM["new_tamgiusotienthuhoach"] = tamgiuSotienTH;
                    }
                    if (CSTM.Attributes.Contains("new_tamgiutoidavanchuyen"))  // tạm giữ tối đa TH
                    {
                        Money tamgiuTDTH = (Money)CSTM["new_tamgiutoidathuhoach"];
                        newCSTM["new_tamgiutoidathuhoach"] = tamgiuTDTH;
                    }

                    Guid newCSTMID = service.Create(newCSTM);

                    // Nhóm khách hàng

                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listNhomKH = new EntityReferenceCollection();

                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                    {
                        foreach (Entity nhomKH in dsNhomKH.Entities)
                        {
                            listNhomKH.Add(nhomKH.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_chinhsachthumua_new_nhomkhachhang"), listNhomKH);
                    }

                    // Vùng địa lý
                    EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachthumua", "new_new_chinhsachthumua_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listVungDl = new EntityReferenceCollection();
                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                    {
                        foreach (Entity vungDL in dsVungDL.Entities)
                        {
                            listVungDl.Add(vungDL.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_chinhsachthumua_new_vung"), listVungDl);
                    }

                    // Nhóm năng suất
                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listNhomNS = new EntityReferenceCollection();
                    if (dsNhomNS != null && dsNhomNS.Entities.Count > 0)
                    {
                        foreach (Entity nhomNS in dsNhomNS.Entities)
                        {
                            listNhomNS.Add(nhomNS.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_chinhsachthumua_new_nhomnangsuat"), listNhomNS);
                    }

                    // Giống mía
                    EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachthumua", "new_new_chinhsachthumua_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listGiongmia = new EntityReferenceCollection();
                    if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                    {
                        foreach (Entity giongmia in dsGiongmia.Entities)
                        {
                            listGiongmia.Add(giongmia.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_chinhsachdautu_new_giongmia"), listGiongmia);
                    }

                    // Khuyến khích phát triển
                    EntityCollection dsKKPTCSTM = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachthumua", "new_new_csthumua_new_khuyenkhichphattrien", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listKKPT = new EntityReferenceCollection();
                    if (dsKKPTCSTM != null && dsKKPTCSTM.Entities.Count > 0)
                    {
                        foreach (Entity kkpt in dsKKPTCSTM.Entities)
                        {
                            listKKPT.Add(kkpt.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_csthumua_new_khuyenkhichphattrien"), listKKPT);
                    }

                    // Nhóm cự ly
                    EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listNhomcl = new EntityReferenceCollection();
                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                    {
                        foreach (Entity nhomcl in dsNHomCL.Entities)
                        {
                            listNhomcl.Add(nhomcl.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_chinhsachthumua_new_nhomculy"), listNhomcl);
                    }

                    // Mô hình khuyến nông
                    EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachthumua", "new_new_chinhsachthumua_new_mohinhkhuyennon", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachthumuaid", CSTM.Id);
                    EntityReferenceCollection listMHKN = new EntityReferenceCollection();
                    if (dsMHKN != null && dsMHKN.Entities.Count > 0)
                    {
                        foreach (Entity mhkn in dsMHKN.Entities)
                        {
                            listMHKN.Add(mhkn.ToEntityReference());
                        }
                        service.Associate("new_chinhsachthumua", newCSTMID, new Relationship("new_new_chinhsachthumua_new_mohinhkhuyennong"), listMHKN);
                    }

                    EntityReference cstmEntityRef = new EntityReference("new_chinhsachthumua", newCSTMID);

                    // Chính sách thu mua CCS

                    EntityCollection ThumuaCCSCol = FindcsCCSBao(service, CSTM);
                    if (ThumuaCCSCol != null && ThumuaCCSCol.Entities.Count > 0)
                    {
                        foreach (Entity a in ThumuaCCSCol.Entities)
                        {
                            Entity ccsbao = new Entity("new_chinhsachthumua_ccsbao");

                            string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Chính sách ccs bao");
                            ccsbao["new_name"] = ten;

                            if (a.Attributes.Contains("new_operatortu"))
                            {
                                int pttu = ((OptionSetValue)a["new_operatortu"]).Value;
                                ccsbao["new_operatortu"] = new OptionSetValue(pttu);
                            }
                            if (a.Attributes.Contains("new_operatorden"))
                            {
                                int ptden = ((OptionSetValue)a["new_operatorden"]).Value;
                                ccsbao["new_operatorden"] = new OptionSetValue(ptden);
                            }

                            decimal tu = (a.Contains("new_tu") ? (decimal)a["new_tu"] : 0);
                            decimal den = (a.Contains("new_den") ? (decimal)a["new_den"] : 0);
                            decimal giatriCCS = (a.Contains("new_giatriccs") ? (decimal)a["new_giatriccs"] : 0);

                            ccsbao["new_tu"] = tu;
                            ccsbao["new_den"] = den;
                            ccsbao["new_giatriccs"] = giatriCCS;
                            ccsbao["new_chinhsachthumua"] = cstmEntityRef;

                            service.Create(ccsbao);
                        }
                    }

                    // Chính sách thu mua CCS thưởng

                    EntityCollection ThumuaCCSThuongCol = FindcsCCSThuong(service, CSTM);
                    if (ThumuaCCSThuongCol != null && ThumuaCCSThuongCol.Entities.Count > 0)
                    {
                        foreach (Entity a in ThumuaCCSThuongCol.Entities)
                        {
                            Entity ccsthuong = new Entity("new_chinhsachthumua_ccsthuong");
                            EntityReference currencyRef = a.GetAttributeValue<EntityReference>("transactioncurrencyid");

                            string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Chính sách ccs thưởng");
                            ccsthuong["new_name"] = ten;

                            if (a.Attributes.Contains("new_nguontien"))
                            {
                                int nguontien = ((OptionSetValue)a["new_nguontien"]).Value;
                                ccsthuong["new_nguontien"] = new OptionSetValue(nguontien);
                            }

                            if (a.Attributes.Contains("new_operatortu"))
                            {
                                int pttu = ((OptionSetValue)a["new_operatortu"]).Value;
                                ccsthuong["new_operatortu"] = new OptionSetValue(pttu);
                            }
                            if (a.Attributes.Contains("new_operatorden"))
                            {
                                int ptden = ((OptionSetValue)a["new_operatorden"]).Value;
                                ccsthuong["new_operatorden"] = new OptionSetValue(ptden);
                            }

                            decimal tu = (a.Contains("new_tu") ? (decimal)a["new_tu"] : 0);
                            decimal den = (a.Contains("new_den") ? (decimal)a["new_den"] : 0);
                            decimal tienthuong = (a.Contains("new_tienthuong") ? ((Money)a["new_tienthuong"]).Value : 0);
                            Money Mtienthuong = new Money(tienthuong);

                            ccsthuong["new_tu"] = tu;
                            ccsthuong["new_den"] = den;
                            ccsthuong["new_tienthuong"] = Mtienthuong;
                            ccsthuong["transactioncurrencyid"] = currencyRef;
                            ccsthuong["new_chinhsachthumua"] = cstmEntityRef;

                            service.Create(ccsthuong);
                        }
                    }

                    EntityCollection TapchatThuongCol = FindcsTapChatThuong(service, CSTM);
                    if (TapchatThuongCol != null && TapchatThuongCol.Entities.Count > 0)
                    {
                        foreach (Entity a in TapchatThuongCol.Entities)
                        {
                            Entity tcthuong = new Entity("new_chinhsachthumuatapchatthuong");
                            EntityReference currencyRef = a.GetAttributeValue<EntityReference>("transactioncurrencyid");

                            string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Chính sách tạp chất thưởng");
                            tcthuong["new_name"] = ten;

                            int nguontien = ((OptionSetValue)a["new_nguontien"]).Value;
                            tcthuong["new_nguontien"] = new OptionSetValue(nguontien);

                            if (a.Attributes.Contains("new_operatortu"))
                            {
                                int pttu = ((OptionSetValue)a["new_operatortu"]).Value;
                                tcthuong["new_operatortu"] = new OptionSetValue(pttu);
                            }
                            if (a.Attributes.Contains("new_operatorden"))
                            {
                                int ptden = ((OptionSetValue)a["new_operatorden"]).Value;
                                tcthuong["new_operatorden"] = new OptionSetValue(ptden);
                            }

                            decimal tu = (a.Contains("new_tu") ? (decimal)a["new_tu"] : 0);
                            decimal den = (a.Contains("new_den") ? (decimal)a["new_den"] : 0);
                            decimal tienthuong = (a.Contains("new_tienthuong") ? ((Money)a["new_tienthuong"]).Value : 0);
                            Money Mtienthuong = new Money(tienthuong);

                            tcthuong["new_tu"] = tu;
                            tcthuong["new_den"] = den;
                            tcthuong["new_tienthuong"] = Mtienthuong;
                            tcthuong["transactioncurrencyid"] = currencyRef;
                            tcthuong["new_chinhsachthumua"] = cstmEntityRef;

                            service.Create(tcthuong);
                        }
                    }

                    EntityCollection TapchatTruCol = FindcsTapChatTru(service, CSTM);
                    if (TapchatTruCol != null && TapchatTruCol.Entities.Count > 0)
                    {
                        foreach (Entity a in TapchatTruCol.Entities)
                        {
                            Entity tctru = new Entity("new_chinhsachthumuatapchattru");

                            string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Chính sách tạp chất trừ");
                            tctru["new_name"] = ten;

                            if (a.Attributes.Contains("new_operatortu"))
                            {
                                int pttu = ((OptionSetValue)a["new_operatortu"]).Value;
                                tctru["new_operatortu"] = new OptionSetValue(pttu);
                            }
                            if (a.Attributes.Contains("new_operatorden"))
                            {
                                int ptden = ((OptionSetValue)a["new_operatorden"]).Value;
                                tctru["new_operatorden"] = new OptionSetValue(ptden);
                            }
                            if (a.Attributes.Contains("new_cachtru"))
                            {
                                int cachtru = ((OptionSetValue)a["new_operatorden"]).Value;
                                tctru["new_cachtru"] = new OptionSetValue(cachtru);
                            }

                            decimal tu = (a.Contains("new_tu") ? (decimal)a["new_tu"] : 0);
                            decimal den = (a.Contains("new_den") ? (decimal)a["new_den"] : 0);
                            decimal heso = (a.Contains("new_heso") ? (decimal)a["new_heso"] : 0);

                            tctru["new_tu"] = tu;
                            tctru["new_den"] = den;
                            tctru["new_heso"] = heso;
                            tctru["new_chinhsachthumua"] = cstmEntityRef;

                            service.Create(tctru);
                        }
                    }

                    // Chính sách thu mua khuyến khích phát triển và NS đường cao
                    if (CSTM.Attributes.Contains("new_hoatdongapdung") && CSTM.GetAttributeValue<OptionSetValue>("new_hoatdongapdung").Value.ToString() == "100000004")
                    {
                        EntityCollection kkptCol = FindcsKK(service, CSTM);
                        if (kkptCol != null && kkptCol.Entities.Count > 0)
                        {
                            foreach (Entity a in kkptCol.Entities)
                            {
                                Entity kkpt = new Entity("new_chinhsachthumua_khuyenkhichphattrien");

                                string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Chính sách khuyến khích phát triển");
                                kkpt["new_name"] = ten;

                                if (a.Attributes.Contains("new_phuongthuctinhtu"))
                                {
                                    int pttu = ((OptionSetValue)a["new_phuongthuctinhtu"]).Value;
                                    kkpt["new_phuongthuctinhtu"] = new OptionSetValue(pttu);
                                }
                                if (a.Attributes.Contains("new_phuongthuctinhden"))
                                {
                                    int ptden = ((OptionSetValue)a["new_phuongthuctinhden"]).Value;
                                    kkpt["new_phuongthuctinhden"] = new OptionSetValue(ptden);
                                }

                                decimal nstu = (a.Contains("new_nangsuattu") ? (decimal)a["new_nangsuattu"] : 0);
                                decimal nsden = (a.Contains("new_nangsuatden") ? (decimal)a["new_nangsuatden"] : 0);
                                if (a.Attributes.Contains("new_dinhmuc"))
                                {
                                    Money dinhmuc = (Money)a["new_dinhmuc"];
                                    kkpt["new_dinhmuc"] = dinhmuc;
                                }

                                kkpt["new_nangsuattu"] = nstu;
                                kkpt["new_nangsuatden"] = nsden;
                                kkpt["new_chinhsachthumua"] = cstmEntityRef;

                                service.Create(kkpt);
                            }
                        }

                        EntityCollection nsduongCol = FindcsNSduong(service, CSTM);
                        if (nsduongCol != null && nsduongCol.Entities.Count > 0)
                        {
                            foreach (Entity a in nsduongCol.Entities)
                            {
                                Entity nsduong = new Entity("new_chinhsachthumua_kknangsuatduongcao");

                                string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Chính sách năng suất đường cao");
                                nsduong["new_name"] = ten;

                                if (a.Attributes.Contains("new_phuongthuctinhtu"))
                                {
                                    int pttu = ((OptionSetValue)a["new_phuongthuctinhtu"]).Value;
                                    nsduong["new_phuongthuctinhtu"] = new OptionSetValue(pttu);
                                }
                                if (a.Attributes.Contains("new_phuongthuctinhden"))
                                {
                                    int ptden = ((OptionSetValue)a["new_phuongthuctinhden"]).Value;
                                    nsduong["new_phuongthuctinhden"] = new OptionSetValue(ptden);
                                }

                                decimal nstu = (a.Contains("new_nangsuattu") ? (decimal)a["new_nangsuattu"] : 0);
                                decimal nsden = (a.Contains("new_nangsuatden") ? (decimal)a["new_nangsuatden"] : 0);
                                decimal tyle = (a.Contains("new_phantramtyle") ? (decimal)a["new_phantramtyle"] : 0);

                                if (a.Attributes.Contains("new_dongiakhuyenkhich"))
                                {
                                    Money dongiakk = (Money)a["new_dongiakhuyenkhich"];
                                    nsduong["new_dongiakhuyenkhich"] = dongiakk;
                                }

                                nsduong["new_nangsuattu"] = nstu;
                                nsduong["new_nangsuatden"] = nsden;
                                nsduong["new_chinhsachthumua"] = cstmEntityRef;
                                nsduong["new_tylemiachay"] = (bool)a["new_tylemiachay"];

                                if (a.Attributes.Contains("new_phuongthuctinh_miachay"))
                                {
                                    int ptmiachay = ((OptionSetValue)a["new_phuongthuctinh_miachay"]).Value;
                                    nsduong["new_phuongthuctinh_miachay"] = new OptionSetValue(ptmiachay);
                                }

                                nsduong["new_phantramtyle"] = tyle;
                                nsduong["new_viphamhopdong"] = (bool)a["new_viphamhopdong"];

                                service.Create(nsduong);
                            }
                        }
                    }

                    //--- END--- Chính sách thu mua khuyến khích phát triển và NS đường cao

                } //if (newvuDT != null && newvuDT.Id != Guid.Empty)  
                else
                {
                    throw new InvalidPluginExecutionException("Chưa có vụ đầu tư mới");
                }

                //traceService.Trace("ID là " + newCSDTID.ToString());

            }//using
        }

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        public static Entity FindvuDT(IOrganizationService crmservices, Entity vudautu)
        {
            string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_vudautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_mavudautu' />
                                        <attribute name='new_ngaybatdau' />
                                        <attribute name='new_ngayketthuc' />
                                        <attribute name='new_dactinh' />
                                        <attribute name='new_danghoatdong' />
                                        <attribute name='new_vudautuid' />
                                        <order attribute='new_ngaybatdau' descending='false' />
                                        <filter type='and'>
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_ngaybatdau' operator='on-or-after' value='{0}' />
                                        </filter>
                                      </entity>
                                    </fetch>";

            DateTime ngaybatdau = (DateTime)vudautu["new_ngaybatdau"];
            fetchXml = string.Format(fetchXml, ngaybatdau);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            if (entc.Entities.Count() >= 2)
            {
                return entc.Entities[1];
            }
            else
            {
                return null;
            }
        }

        public static EntityCollection FindcsCCSBao(IOrganizationService crmservices, Entity CSTM)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachthumua_ccsbao'>
                        <attribute name='new_tu' />
                        <attribute name='new_operatortu' />
                        <attribute name='new_operatorden' />
                        <attribute name='new_den' />
                        <attribute name='new_name' />
                        <attribute name='new_chinhsachthumua' />
                        <attribute name='new_chinhsachthumua_ccsbaoid' />
                        <order attribute='new_tu' descending='false' />
                        <filter type='and'>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_chinhsachthumua' operator='eq' uitype='new_chinhsachthumua' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, CSTM.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindcsCCSThuong(IOrganizationService crmservices, Entity CSTM)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachthumua_ccsthuong'>
                        <attribute name='new_tu' />
                        <attribute name='new_tienthuong' />
                        <attribute name='new_nguontien' />
                        <attribute name='new_operatortu' />
                        <attribute name='new_operatorden' />
                        <attribute name='new_den' />
                        <attribute name='new_name' />
                        <attribute name='new_chinhsachthumua' />
                        <attribute name='new_chinhsachthumua_ccsthuongid' />
                        <order attribute='new_tu' descending='false' />
                        <filter type='and'>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_chinhsachthumua' operator='eq' uitype='new_chinhsachthumua' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, CSTM.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindcsTapChatThuong(IOrganizationService crmservices, Entity CSTM)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachthumuatapchatthuong'>
                        <attribute name='new_name' />
                        <attribute name='new_tu' />
                        <attribute name='new_nguontien' />
                        <attribute name='new_tienthuong' />
                        <attribute name='new_operatortu' />
                        <attribute name='new_operatorden' />
                        <attribute name='new_den' />
                        <attribute name='new_chinhsachthumua' />
                        <attribute name='new_chinhsachthumuatapchatthuongid' />
                        <order attribute='new_tu' descending='false' />
                        <filter type='and'>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_chinhsachthumua' operator='eq' uitype='new_chinhsachthumua' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, CSTM.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindcsTapChatTru(IOrganizationService crmservices, Entity CSTM)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachthumuatapchattru'>
                        <attribute name='new_name' />
                        <attribute name='new_tu' />
                        <attribute name='new_operatortu' />
                        <attribute name='new_operatorden' />
                        <attribute name='new_heso' />
                        <attribute name='new_den' />
                        <attribute name='new_chinhsachthumua' />
                        <attribute name='new_cachtru' />
                        <attribute name='new_chinhsachthumuatapchattruid' />
                        <order attribute='new_tu' descending='false' />
                        <filter type='and'>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_chinhsachthumua' operator='eq' uitype='new_chinhsachthumua' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, CSTM.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindcsKK(IOrganizationService crmservices, Entity CSTM)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachthumua_khuyenkhichphattrien'>
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_phuongthuctinhtu' />
                        <attribute name='new_phuongthuctinhden' />
                        <attribute name='new_nangsuattu' />
                        <attribute name='new_nangsuatden' />
                        <attribute name='new_dinhmuc' />
                        <attribute name='new_chinhsachthumua_khuyenkhichphattrienid' />
                        <order attribute='new_nangsuattu' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chinhsachthumua' operator='eq' uitype='new_chinhsachthumua' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, CSTM.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindcsNSduong(IOrganizationService crmservices, Entity CSTM)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachthumua_kknangsuatduongcao'>
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_viphamhopdong' />
                        <attribute name='new_tylemiachay' />
                        <attribute name='new_nangsuattu' />
                        <attribute name='new_nangsuatden' />
                        <attribute name='new_dongiakhuyenkhich' />
                        <attribute name='new_chinhsachthumua_kknangsuatduongcaoid' />
                        <order attribute='new_nangsuattu' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chinhsachthumua' operator='eq' uitype='new_chinhsachthumua' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, CSTM.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }
        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }
    }
}


//StringBuilder xml = new StringBuilder();
// xml.AppendLine("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
// xml.AppendLine()
//stringA.AppendLine(stringB) nghia la stringA = stringA + stringB

// muốn check field có value không thì dùng service.Trace(entityA.Attributes.Contains("fieldxyz")); 
// sau đó dùng hàm Contains() để check coi nó có value ko
// hoặc dùng if (bien == null) Trace("A") else Trace("B");

//Logger.Write("Phonecall PostCreate", "Begin");
//throw new InvalidPluginExecutionException("End");
//Logger.Write("entity Id", entity.Id.ToString());

//Convert.ChangeType(mCSTM["new_dongiatang1ccs"]), decimal);
//giá trị mới = Convert.ChangeType(val, pd.PropertyType);
//service.Trace(entityA.Attributes.Contains("fieldxyz"));
//service.Trace("vi tri 1");