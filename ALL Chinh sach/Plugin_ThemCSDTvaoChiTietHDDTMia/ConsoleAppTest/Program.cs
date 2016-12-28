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
                Guid entityId = new Guid("3467E005-F524-E611-80C0-9ABE942A7CB1");//{472AA958-BFB5-E511-93F1-9ABE942A7E29} 24EB7CBB-B5B5-E511-93F1-9ABE942A7E29 
                Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", entityId, new ColumnSet(new string[] { "new_name", "new_copytuhddtthuedat" }));

                //Guid entityId = ChiTietHD.Id;

                //if (ChiTietHD.Contains("new_thuadatcanhtac"))
                //{
                //traceService.Trace("Begin plugin");
                //if (context.MessageName.ToUpper() == "CREATE")

                ChiTietHD = service.Retrieve("new_thuadatcanhtac", entityId, new ColumnSet(new string[] { "new_vutrong", "new_loaisohuudat","new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu", "new_copytuhddtthuedat" }));
                DateTime ngaytao = ChiTietHD.GetAttributeValue<DateTime>("createdon");

                EntityReference thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                Guid thuadatId = thuadatEntityRef.Id;
                Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));

                EntityReference giongmiaEntityRef = null;
                Guid giongmiaId = new Guid();
                Entity giongmiaObj = null;
                if (ChiTietHD.Attributes.Contains("new_giongmia"))
                {
                    giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                    giongmiaId = giongmiaEntityRef.Id;
                    giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                }

                EntityReference HDDTmiaRef = ChiTietHD.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                Guid DHDTmiaId = HDDTmiaRef.Id;
                Entity HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu" }));

                EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                if (vudautuRef == null || vudautuRef.Id == Guid.Empty)
                {
                    throw new InvalidPluginExecutionException("Trong HĐĐT mía chưa có Vụ đầu tư !");
                }
                else
                {
                    Guid vuDTId = vudautuRef.Id;
                    EntityCollection resultCol = FindCSDTtrongmia(service, ChiTietHD);
                    Entity mCSDT = null;

                    if (resultCol != null && resultCol.Entities.Count > 0)
                    {
                        #region find csdt
                        foreach (Entity a in resultCol.Entities)
                        {
                            if (a.Contains("new_vutrong_vl"))  // Vu trong
                            {
                                if (ChiTietHD.Contains("new_vutrong"))
                                {
                                    if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_vutrong"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                            {
                                if (ChiTietHD.Contains("new_loaigocmia"))
                                {
                                    if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaigocmia"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                            {
                                if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                {
                                    if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                            {
                                if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                {
                                    if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                            {
                                if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                {
                                    if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                            {
                                if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                {
                                    if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            if (a.Contains("new_luugoc"))  // Luu goc
                            {
                                if (ChiTietHD.Attributes.Contains("new_luugoc"))
                                {
                                    if (((OptionSetValue)a["new_luugoc"]).Value.ToString() != ((OptionSetValue)ChiTietHD["new_luugoc"]).Value.ToString())
                                        continue;
                                }
                                else
                                    continue;
                            }

                            //traceService.Trace("tuoi mia CTHD " + (bool)ChiTietHD["new_tuoimia"]);
                            //traceService.Trace("tuoi mia CSDT " + (bool)a["new_tuoi"]);
                            //if (a.Contains("new_tuoi"))  // Tuoi mia
                            //{
                            //    if (ChiTietHD.Attributes.Contains("new_tuoimia"))
                            //    {
                            //        if ((bool)a["new_tuoi"] != (bool)ChiTietHD["new_tuoimia"])
                            //            continue;
                            //    }
                            //    else
                            //        continue;
                            //}

                            // NHom khach hang
                            bool co = false;

                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                            {
                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomKH in dsNhomKH.Entities)
                                        {
                                            if (nhomKHHDCT.Id == nhomKH.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomKH trong CTHD
                                {

                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomKH in dsNhomKH.Entities)
                                        {
                                            if (nhomKHHDCT.Id == nhomKH.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomKH trong CTHD
                                {

                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }
                            if (co == false)
                                continue;

                            //Vung dia ly
                            co = false;

                            EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                            if (thuadatObj.Attributes.Contains("new_vungdialy"))
                            {
                                Guid vungdlId = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                {
                                    foreach (Entity vungDL1 in dsVungDL.Entities)
                                    {
                                        if (vungDL.Id == vungDL1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co VungDiaLy trong CTHD
                            {

                                if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // Giong mia
                            co = false;

                            EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", a.Id);
                            if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                            {
                                foreach (Entity giongmia in dsGiongmia.Entities)
                                {
                                    if (giongmiaObj.Id == giongmia.Id)
                                    {
                                        co = true;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                co = true;
                            }
                            if (co == false)
                                continue;

                            // Khuyen khich phat trien
                            co = false;
                            EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                            EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", a.Id);

                            if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                            {
                                if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                {
                                    foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                    {
                                        foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                        {
                                            //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                            if (kkpt1.Id == kkpt2.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                        if (co)
                                        {
                                            //thoat vong for thu 1
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co KKPT trong CTHD
                            {

                                if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // Nhom cu ly
                            co = false;

                            EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                            if (thuadatObj.Attributes.Contains("new_nhomculy"))
                            {
                                Guid nhomclId = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                {
                                    foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                    {
                                        if (nhomCL.Id == nhomCL1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co NHomCL trong CTHD
                            {

                                if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // Mo hinh khuyen nong
                            co = false;

                            EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                            if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                            {
                                EntityReference mhknEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                Guid mhknId = mhknEntityRef.Id;
                                Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                {
                                    foreach (Entity mhkn1 in dsMHKN.Entities)
                                    {
                                        if (mhkn.Id == mhkn1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co MNKH trong CTHD
                            {
                                if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // NHom nang suat
                            co = false;

                            EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                            {
                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsNhomNS.Entities)
                                        {
                                            if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else
                                {
                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }
                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsNhomNS.Entities)
                                        {
                                            if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else
                                {
                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (co == false)
                                continue;


                            mCSDT = a;
                            break;
                        }
                        #endregion
                    }
                    
                    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    {
                        #region found csdt
                        // ------Gan vao Chi tiet HDDT mia
                        EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                        if (ChiTietHD.Contains("new_chinhsachdautu"))
                            ChiTietHD.Attributes["new_chinhsachdautu"] = csdtRef;
                        else
                            ChiTietHD.Attributes.Add("new_chinhsachdautu", csdtRef);

                        // -------Gan ty le thu hoi von du kien
                        // Lay nhung tylethuhoivon trong chinh sach dau tu
                        string fetchTLTHV =
                        @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                  <entity name='new_tilethuhoivon'>
                                    <attribute name='new_name' />
                                    <attribute name='new_phantramtilethuhoi' />
                                    <attribute name='new_nam' />
                                    <attribute name='new_chinhsachdautu' />
                                    <attribute name='new_sotien' />
                                    <attribute name='new_tilethuhoivonid' />
                                    <order attribute='new_nam' descending='false' />
                                    <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                      <filter type='and'>
                                        <condition attribute='statecode' operator='eq' value='0' />
                                        <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                      </filter>
                                    </link-entity>
                                  </entity>
                                </fetch>";

                        Guid csdtKQ = mCSDT.Id;

                        fetchTLTHV = string.Format(fetchTLTHV, csdtKQ);
                        EntityCollection collTLTHV = service.RetrieveMultiple(new FetchExpression(fetchTLTHV));
                        List<Entity> listTLTHV = collTLTHV.Entities.ToList<Entity>();
                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));

                        foreach (Entity TLTHV in listTLTHV)
                        {
                            Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                            EntityReference vudautuEntityRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                            EntityReference hdctEntityRef = new EntityReference("new_thuadatcanhtac", entityId);

                            if (TLTHV.Attributes.Contains("new_phantramtilethuhoi") && TLTHV.Attributes.Contains("new_nam") && csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                            {
                                string tenTLTHVDK = "Năm " + TLTHV.GetAttributeValue<int>("new_nam").ToString();
                                decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi") ? (decimal)TLTHV["new_phantramtilethuhoi"] : 0);
                                decimal dientichtt = (ChiTietHD.Contains("new_dientichthucte") ? (decimal)ChiTietHD["new_dientichthucte"] : 0);
                                decimal dinhmucDThl = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                decimal sotien = 0;

                                sotien = (dinhmucDThl * dientichtt * tyle) / 100;

                                Money sotienM = new Money(sotien);

                                tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000000));
                                tlthvdkHDCT.Attributes.Add("new_chitiethddtmia", hdctEntityRef);
                                tlthvdkHDCT.Attributes.Add("new_vudautu", vudautuEntityRef);
                                tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                service.Create(tlthvdkHDCT);
                            }
                        }
                        // ------End Gan vao ty le thu hoi von du kien

                        //EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                        //Guid vuDTId = vudautuRef.Id;

                        // Lay thong so vu dau tu
                        string fetchTSVDT =
                          @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                  <entity name='new_thongsotheovudautu'>
                                    <attribute name='new_name' />
                                    <attribute name='createdon' />
                                    <attribute name='new_vudautu' />
                                    <attribute name='new_loai' />
                                    <attribute name='new_giatri' />
                                    <attribute name='new_giatien' />
                                    <attribute name='new_apdungtu' />
                                    <attribute name='new_thongsotheovudautuid' />
                                    <order attribute='new_name' descending='false' />
                                    <filter type='and'>
                                      <condition attribute='statecode' operator='eq' value='0' />
                                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                                    </filter>
                                  </entity>
                                </fetch>";

                        fetchTSVDT = string.Format(fetchTSVDT, vuDTId);
                        EntityCollection collTSVDT = service.RetrieveMultiple(new FetchExpression(fetchTSVDT));
                        List<Entity> listTSVDT = collTSVDT.Entities.ToList<Entity>();

                        // ------ Gan NHom du lieu Lai suat
                        if (collTSVDT.Entities.Count > 0)
                        {
                            // Loai lai suat
                            if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                            {
                                bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");

                                if (ChiTietHD.Attributes.Contains("new_loailaisuat"))  // Loai lai suat 100,000,000: co dinh  /// 100,000,001: thay doi
                                {
                                    if (loails == false) // ls thay doi
                                    {
                                        ChiTietHD.Attributes["new_loailaisuat"] = new OptionSetValue(100000001);
                                    }
                                    else   // ls co dinh
                                    {
                                        ChiTietHD.Attributes["new_loailaisuat"] = new OptionSetValue(100000000);
                                    }
                                }
                                else
                                {
                                    if (loails == false)
                                    {
                                        ChiTietHD.Attributes.Add("new_loailaisuat", new OptionSetValue(100000001));
                                    }
                                    else
                                    {
                                        ChiTietHD.Attributes.Add("new_loailaisuat", new OptionSetValue(100000000));
                                    }
                                }
                            }

                            // Muc lai suat

                            if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                            {
                                bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");
                                if (loails == false)   // ls thay doi
                                {
                                    decimal mucls = (csdtKQEntity.Contains("new_muclaisuatdautu") ? (decimal)csdtKQEntity["new_muclaisuatdautu"] : 0);

                                    if (ChiTietHD.Attributes.Contains("new_laisuat"))
                                    {
                                        ChiTietHD.Attributes["new_laisuat"] = mucls;
                                    }
                                    else
                                    {
                                        ChiTietHD.Attributes.Add("new_laisuat", mucls);
                                    }
                                }
                                else // ls co dinh
                                {
                                    foreach (Entity TSVDT in listTSVDT)
                                    {
                                        if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000001) //100,000,001 : Loai ls
                                        {
                                            if (TSVDT.Attributes.Contains("new_giatri"))
                                            {
                                                decimal mucls = TSVDT.GetAttributeValue<decimal>("new_giatri");
                                                if (ChiTietHD.Attributes.Contains("new_laisuat"))
                                                {
                                                    ChiTietHD.Attributes["new_laisuat"] = mucls;
                                                }
                                                else
                                                {
                                                    ChiTietHD.Attributes.Add("new_laisuat", mucls);
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            if (csdtKQEntity.Attributes.Contains("new_cachtinhlai"))
                            {
                                OptionSetValue cachlinhlai = csdtKQEntity.GetAttributeValue<OptionSetValue>("new_cachtinhlai");
                                if (ChiTietHD.Attributes.Contains("new_cachtinhlai"))   // Cach tinh lai
                                {
                                    ChiTietHD.Attributes["new_cachtinhlai"] = cachlinhlai;
                                }
                                else
                                {
                                    ChiTietHD.Attributes.Add("new_cachtinhlai", cachlinhlai);
                                }
                            }

                            // ------ End nhom du lieu Gan Lai suat

                            // -------- Gan nhom du lieu  Dinh muc

                            foreach (Entity TSVDT in listTSVDT)       // Gia mia du kien
                            {
                                if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000004) //100,000,004 : Gia mia du kien
                                {
                                    if (TSVDT.Attributes.Contains("new_giatien"))
                                    {
                                        Money giamiadk = TSVDT.GetAttributeValue<Money>("new_giatien");
                                        if (ChiTietHD.Attributes.Contains("new_giamiadukien"))  // Gia mia du kien
                                        {
                                            ChiTietHD.Attributes["new_giamiadukien"] = giamiadk;
                                        }
                                        else
                                        {
                                            ChiTietHD.Attributes.Add("new_giamiadukien", giamiadk);
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        // ----------------- DINH MUC KHONG HOAN LAI

                        decimal dinhmucbs = 0;

                        // ------------ Tìm CSDT bổ sung
                        string fetchCSDTBS =
                              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                          <entity name='new_chinhsachdautuchitiet'>
                                            <attribute name='new_chinhsachdautuchitietid' />
                                            <attribute name='new_name' />
                                            <attribute name='createdon' />
                                            <attribute name='new_nhomkhachhang' />
                                            <attribute name='new_giongmia' />
                                            <attribute name='new_nhomnangsuat' />
                                            <attribute name='new_khuyenkhichphattrien' />
                                            <attribute name='new_mohinhkhuyennong' />
                                            <attribute name='new_nhomculy' />
                                            <attribute name='new_vungdialy' />
                                            <attribute name='new_sotienbosung' />
                                            <order attribute='new_name' descending='false' />
                                            <filter type='and'>
                                              <condition attribute='statecode' operator='eq' value='0' />
                                              <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                              <condition attribute='new_tungay' operator='on-or-before' value='{1}' />
                                              <condition attribute='new_denngay' operator='on-or-after' value='{2}' />
                                            </filter>
                                          </entity>
                                        </fetch>";

                        fetchCSDTBS = string.Format(fetchCSDTBS, vuDTId, ngaytao, ngaytao);
                        EntityCollection resultCSDTBS = service.RetrieveMultiple(new FetchExpression(fetchCSDTBS));

                        foreach (Entity csdtbs in resultCSDTBS.Entities)
                        {
                            // NHom khach hang
                            bool phuhop = true;
                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                            {
                                if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    EntityReference nhomkhCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang" }));

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;

                                        if (nhomkhId != nhomkhCSDTBSId)
                                        {
                                            phuhop = false;
                                        }
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        phuhop = false;
                                    }

                                }
                            }

                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    EntityReference nhomkhCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;

                                        if (nhomkhId != nhomkhCSDTBSId)
                                        {
                                            phuhop = false;
                                        }
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        phuhop = false;
                                    }
                                }
                            }

                            if (phuhop == false)
                                continue;

                            // Giong mia

                            phuhop = true;
                            if (csdtbs.Attributes.Contains("new_giongmia"))
                            {
                                EntityReference giongmiaCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_giongmia");
                                if (giongmiaEntityRef != null && giongmiaEntityRef.Id != Guid.Empty)
                                {
                                    Guid giongmiaCSDTBSId = giongmiaCSDTBSRef.Id;

                                    if (giongmiaId != giongmiaCSDTBSId)
                                    {
                                        phuhop = false;
                                    }
                                }
                                else   //neu khong co Giongmia trong CTHD
                                {
                                    phuhop = false;
                                }

                            }
                            if (phuhop == false)
                                continue;

                            // NHom nang suat

                            phuhop = true;
                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                            {
                                if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                {
                                    EntityReference nhomnangsuatCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                    Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                    Entity nhomnangsuatCSDTBS = service.Retrieve("new_nhomnangsuat", nhomnangsuatCSDTBSRefId, new ColumnSet(new string[] { "new_nangsuattu", "new_nangsuatden" }));

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        decimal nangsuattu = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuattu");
                                        decimal nangsuatden = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuatden");

                                        if (!((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden)))
                                        {
                                            phuhop = false;
                                        }
                                    }
                                    else
                                    {
                                        phuhop = false;
                                    }
                                }
                            }

                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                {
                                    EntityReference nhomnangsuatCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                    Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                    Entity nhomnangsuatCSDTBS = service.Retrieve("new_nhomnangsuat", nhomnangsuatCSDTBSRefId, new ColumnSet(new string[] { "new_nangsuattu", "new_nangsuatden" }));

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        decimal nangsuattu = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuattu");
                                        decimal nangsuatden = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuatden");

                                        if (!((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden)))
                                        {
                                            phuhop = false;
                                        }
                                    }
                                    else
                                    {
                                        phuhop = false;
                                    }
                                }
                            }

                            if (phuhop == false)
                                continue;

                            // Khuyen khich phat trien

                            phuhop = true;
                            EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);

                            if (csdtbs.Attributes.Contains("new_khuyenkhichphattrien"))
                            {
                                EntityReference kkptCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_khuyenkhichphattrien");
                                if (dsKKPTHDCT.Entities.Count > 0)
                                {
                                    foreach (Entity kkptHDCT in dsKKPTHDCT.Entities)
                                    {
                                        if (kkptHDCT.Id != kkptCSDTBSRef.Id)
                                        {
                                            phuhop = false;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    phuhop = false;
                                }
                            }

                            if (phuhop == false)
                                continue;

                            // Mo hinh khuyen nong

                            phuhop = true;

                            if (csdtbs.Attributes.Contains("new_mohinhkhuyennong"))
                            {
                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    EntityReference mhknEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                    EntityReference mhknCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_mohinhkhuyennong");

                                    if (mhknCSDTBSRef.Id != mhknEntityRef.Id)
                                        phuhop = false;
                                }
                                else
                                {
                                    phuhop = false;
                                }

                            }
                            if (phuhop == false)
                                continue;

                            // Nhom cu ly

                            phuhop = true;

                            if (csdtbs.Attributes.Contains("new_nhomculy"))
                            {
                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                {
                                    EntityReference nhomclEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                    EntityReference nhomclCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomculy");
                                    if (nhomclEntityRef.Id != nhomclCSDTBSRef.Id)
                                    {
                                        phuhop = false;
                                    }
                                }
                                else
                                {
                                    phuhop = false;
                                }
                            }
                            if (phuhop == false)
                                continue;

                            //Vung dia ly

                            phuhop = true;

                            if (csdtbs.Attributes.Contains("new_vungdialy"))
                            {
                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                {
                                    EntityReference vungdlEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy");
                                    EntityReference vungdlCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_vungdialy");
                                    if (vungdlEntityRef.Id != vungdlCSDTBSRef.Id)
                                    {
                                        phuhop = false;
                                    }
                                }
                                else
                                {
                                    phuhop = false;
                                }
                            }
                            if (phuhop == false)
                                continue;
                            //break;

                            dinhmucbs += csdtbs.GetAttributeValue<Money>("new_sotienbosung").Value;
                        }

                        decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong") ? (decimal)ChiTietHD["new_dientichhopdong"] : 0);

                        if (csdtKQEntity.Attributes.Contains("new_dinhmucdautukhonghoanlai"))   // Dau tu khong hoan lai
                        {
                            decimal dinhmucDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                            dinhmucDTKHL += dinhmucbs;
                            Money MdinhmucDTKHL = new Money(dinhmucDTKHL);

                            decimal tongDMDTKHL = dinhmucDTKHL * dientichhd;
                            Money MtongDMDTKHL = new Money(tongDMDTKHL);

                            //traceService.Trace("truoc cap nhat " + tongDMDTKHL);
                            if (ChiTietHD.Attributes.Contains("new_dongiadautukhonghoanlai"))
                            {
                                ChiTietHD.Attributes["new_dongiadautukhonghoanlai"] = MdinhmucDTKHL;
                            }
                            else
                            {
                                ChiTietHD.Attributes.Add("new_dongiadautukhonghoanlai", MdinhmucDTKHL);
                            }

                            if (ChiTietHD.Attributes.Contains("new_dinhmucdautukhonghoanlai"))
                            {
                                ChiTietHD.Attributes["new_dinhmucdautukhonghoanlai"] = MtongDMDTKHL;
                            }
                            else
                            {
                                ChiTietHD.Attributes.Add("new_dinhmucdautukhonghoanlai", MtongDMDTKHL);
                            }
                        }
                        // ----------------- DINH MUC KHONG HOAN LAI

                        // Dau tu  hoan lai
                        if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                        {
                            decimal dongiaDTHL = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value;
                            decimal dinhmucDTHL = dongiaDTHL * dientichhd;

                            Money MdongiaDT = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai");
                            Money MdinhmucDT = new Money(dinhmucDTHL);

                            if (ChiTietHD.Attributes.Contains("new_dongiadautuhoanlai"))
                            {
                                ChiTietHD.Attributes["new_dongiadautuhoanlai"] = MdongiaDT;
                            }
                            else
                            {
                                ChiTietHD.Attributes.Add("new_dongiadautuhoanlai", MdongiaDT);
                            }

                            if (ChiTietHD.Attributes.Contains("new_dinhmucdautuhoanlai"))
                            {
                                ChiTietHD.Attributes["new_dinhmucdautuhoanlai"] = MdinhmucDT;
                            }
                            else
                            {
                                ChiTietHD.Attributes.Add("new_dinhmucdautuhoanlai", MdinhmucDT);
                            }
                        }

                        // -------------------- DON GIA PHAN BON

                        decimal dongiaPhanbon = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);
                        if (csdtKQEntity.Attributes.Contains("new_dinhmucphanbontoithieu"))   // don gia phan bon toi thieu
                        {
                            Money MdongiaPhanbon = new Money(dongiaPhanbon);

                            if (ChiTietHD.Attributes.Contains("new_dinhmucphanbontoithieu"))
                            {
                                ChiTietHD.Attributes["new_dongiaphanbontoithieu"] = MdongiaPhanbon;
                            }
                            else
                            {
                                ChiTietHD.Attributes.Add("new_dongiaphanbontoithieu", MdongiaPhanbon);
                            }
                        }
                        // ----------END---------- DON GIA PHAN BON

                        // -------------------- DINH MUC PHAN BON

                        if (csdtKQEntity.Attributes.Contains("new_dinhmucphanbontoithieu"))   // dinh muc phan bon toi thieu
                        {
                            //decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong") ? (decimal)ChiTietHD["new_dientichhopdong"] : 0);

                            decimal tongDMPB = dongiaPhanbon * dientichhd;
                            Money MtongDMDTKHL = new Money(tongDMPB);

                            if (ChiTietHD.Attributes.Contains("new_dinhmucphanbontoithieu"))
                            {
                                ChiTietHD.Attributes["new_dinhmucphanbontoithieu"] = MtongDMDTKHL;
                            }
                            else
                            {
                                ChiTietHD.Attributes.Add("new_dinhmucphanbontoithieu", MtongDMDTKHL);
                            }
                        }

                        // --------END--------- DINH MUC PHAN BON

                        // -------------------- DINH MUC DAU TU

                        decimal dmDTHL = (ChiTietHD.Contains("new_dinhmucdautuhoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                        decimal dmDTKHL = (ChiTietHD.Contains("new_dinhmucdautukhonghoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                        decimal dmPhanbon = (ChiTietHD.Contains("new_dinhmucphanbontoithieu") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                        decimal tongDM = dmDTHL + dmDTKHL + dmPhanbon;
                        Money MtongDM = new Money(tongDM);

                        if (ChiTietHD.Attributes.Contains("new_dinhmucdautu"))
                        {
                            ChiTietHD.Attributes["new_dinhmucdautu"] = MtongDM;
                        }
                        else
                        {
                            ChiTietHD.Attributes.Add("new_dinhmucdautu", MtongDM);
                        }

                        // --------END--------- DINH MUC DAU TU

                        // -------- End nhom du lieu  Gan Dinh muc

                        service.Update(ChiTietHD);

                        EntityReferenceCollection listCSDT = new EntityReferenceCollection();
                        listCSDT.Add(mCSDT.ToEntityReference());
                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDT.ToEntityReference() });

                        //-------- Tìm CSDT thâm canh --------------------

                        EntityCollection csdtThamcanhCol = FindCSDTthamcanh(service, ChiTietHD);
                        Entity mCSDTthamcanh = null;

                        if (csdtThamcanhCol != null && csdtThamcanhCol.Entities.Count > 0)
                        {
                            foreach (Entity a in csdtThamcanhCol.Entities)
                            {
                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                {
                                    if (ChiTietHD.Contains("new_vutrong"))
                                    {
                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_vutrong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                {
                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                    {
                                        if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaigocmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }


                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                {
                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                    {
                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                {
                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                // Nhom khach hang
                                bool co = false;
                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                            co = true;
                                    }
                                }
                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (co == false)
                                    continue;

                                //Vung dia ly
                                co = false;

                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                {
                                    Guid vungdlId = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                    Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                    {
                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                        {
                                            if (vungDL.Id == vungDL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co VungDiaLy trong CTHD
                                {
                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // Giong mia
                                co = false;
                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", a.Id);
                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                {
                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                    {
                                        if (giongmiaObj.Id == giongmia.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                    co = true;

                                if (co == false)
                                    continue;

                                // Khuyen khich phat trien
                                co = false;
                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", a.Id);

                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                {
                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                    {
                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                        {
                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                            {
                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                if (kkpt1.Id == kkpt2.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                            if (co)
                                                break;  //thoat vong for thu 1
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co KKPT trong CTHD
                                {
                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // Nhom cu ly
                                co = false;

                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                {
                                    Guid nhomclId = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                        {
                                            if (nhomCL.Id == nhomCL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co NHomCL trong CTHD
                                {
                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // Mo hinh khuyen nong
                                co = false;

                                EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    Guid mhknId = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                        {
                                            if (mhkn.Id == mhkn1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co MNKH trong CTHD
                                {
                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // NHom nang suat
                                co = false;
                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            co = true;
                                    }
                                }
                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (co == false)
                                    continue;

                                mCSDTthamcanh = a;
                                break;
                            }
                            if (mCSDTthamcanh != null && mCSDTthamcanh.Id != Guid.Empty)
                                listCSDT.Add(mCSDTthamcanh.ToEntityReference());
                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTthamcanh.ToEntityReference() });
                        }
                        //----END---- Tìm CSDT thâm canh --------------------

                        //-------- Tìm CSDT tưới mía --------------------

                        EntityCollection csdtTuoimiaCol = FindCSDTtuoi(service, ChiTietHD);
                        Entity mCSDTtuoimia = null;

                        if (csdtTuoimiaCol != null && csdtTuoimiaCol.Entities.Count > 0)
                        {
                            foreach (Entity a in csdtTuoimiaCol.Entities)
                            {
                                if (a.Contains("new_mucdichtuoi_vl"))  // Muc dich tuoi
                                {
                                    if (ChiTietHD.Attributes.Contains("new_mucdichtuoi"))
                                    {
                                        if (a["new_mucdichtuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichtuoi"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_phuongphaptuoi_vl"))  // Phuong phap tuoi
                                {
                                    if (ChiTietHD.Attributes.Contains("new_phuongphaptuoi"))
                                    {
                                        if (a["new_phuongphaptuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_phuongphaptuoi"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                {
                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                //traceService.Trace("vi trí Nhom KH");
                                // NHom khach hang
                                bool co = false;
                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                            co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                //traceService.Trace("vi trí truoc Vung dia ly");
                                //Vung dia ly
                                co = false;

                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                {
                                    Guid vungdlId = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                    Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                    {
                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                        {
                                            if (vungDL.Id == vungDL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co VungDiaLy trong CTHD
                                {
                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                        co = true;
                                }

                                if (co == false)
                                    continue;

                                //traceService.Trace("vi trí truoc Nhom cu ly");
                                // Nhom cu ly
                                co = false;

                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                {
                                    Guid nhomclId = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                        {
                                            if (nhomCL.Id == nhomCL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                else   //neu khong co NHomCL trong CTHD
                                {
                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                //traceService.Trace("vi trí truoc NHom nang suat");
                                // NHom nang suat
                                co = false;
                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }

                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (co == false)
                                    continue;

                                mCSDTtuoimia = a;
                                break;
                            }
                            if (mCSDTtuoimia != null && mCSDTtuoimia.Id != Guid.Empty)
                                listCSDT.Add(mCSDTtuoimia.ToEntityReference());
                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTtuoimia.ToEntityReference() });
                        }
                        //----END---- Tìm CSDT tưới mía --------------------

                        //-------- Tìm CSDT bóc lá mía --------------------

                        //traceService.Trace("vi trí bat dau Tìm CSDT bóc lá mía");
                        EntityCollection csdtBocLamiaCol = FindCSDTbocla(service, ChiTietHD);
                        Entity mCSDTbocla = null;

                        if (csdtBocLamiaCol != null && csdtBocLamiaCol.Entities.Count > 0)
                        {
                            foreach (Entity a in csdtBocLamiaCol.Entities)
                            {
                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                {
                                    if (ChiTietHD.Contains("new_vutrong"))
                                    {
                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_vutrong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                {
                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                    {
                                        if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaigocmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                {
                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                    {
                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                {
                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                // NHom khach hang
                                bool co = false;
                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (co == false)
                                    continue;

                                //Vung dia ly
                                co = false;

                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                {
                                    Guid vungdlId = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                    Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                    {
                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                        {
                                            if (vungDL.Id == vungDL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co VungDiaLy trong CTHD
                                {
                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // Giong mia
                                co = false;

                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", a.Id);
                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                {
                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                    {
                                        if (giongmiaObj.Id == giongmia.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                    co = true;

                                if (co == false)
                                    continue;

                                // Khuyen khich phat trien
                                co = false;
                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", a.Id);

                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                {
                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                    {
                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                        {
                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                            {
                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                if (kkpt1.Id == kkpt2.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                            if (co)
                                                //thoat vong for thu 1
                                                break;
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co KKPT trong CTHD
                                {
                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // Nhom cu ly
                                co = false;

                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                {
                                    Guid nhomclId = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                        {
                                            if (nhomCL.Id == nhomCL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co NHomCL trong CTHD
                                {
                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // Mo hinh khuyen nong
                                co = false;

                                EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    Guid mhknId = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                    {
                                        //List<Entity> ldsMHKN = dsMHKN.Entities.ToList<Entity>();

                                        //foreach (Entity mhkn1 in ldsMHKN)
                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                        {
                                            if (mhkn.Id == mhkn1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                        co = true;
                                }
                                else   //neu khong co MNKH trong CTHD
                                {
                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                        co = true;
                                }
                                if (co == false)
                                    continue;

                                // NHom nang suat
                                co = false;
                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            co = true;
                                    }
                                }
                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                            co = true;
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                            co = true;
                                    }
                                }

                                if (co == false)
                                    continue;

                                mCSDTbocla = a;
                                break;
                            }
                            if (mCSDTbocla != null && mCSDTbocla.Id != Guid.Empty)
                                listCSDT.Add(mCSDTbocla.ToEntityReference());
                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTbocla.ToEntityReference() }); 
                        }
                        //----END---- Tìm CSDT bóc lá mía --------------------

                        //----------- Tìm CSDT ứng --------------------

                        EntityCollection csdtUngCol = FindCSDTung(service, ChiTietHD);
                        Entity mCSDTung = null;

                        if (csdtUngCol != null && csdtUngCol.Entities.Count > 0)
                        {
                            foreach (Entity a in resultCol.Entities)
                            {
                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                {
                                    if (ChiTietHD.Contains("new_vutrong"))
                                    {
                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_vutrong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                {
                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                    {
                                        if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaigocmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                {
                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                    {
                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                {
                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                }

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                }

                                // NHom khach hang
                                bool co = false;

                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {

                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }

                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                        {
                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                            {
                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else   //neu khong co NHomKH trong CTHD
                                    {

                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }
                                if (co == false)
                                    continue;

                                //Vung dia ly
                                co = false;

                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                {
                                    Guid vungdlId = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                    Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                    {
                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                        {
                                            if (vungDL.Id == vungDL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co VungDiaLy trong CTHD
                                {

                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Giong mia
                                co = false;

                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", a.Id);
                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                {
                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                    {
                                        if (giongmiaObj.Id == giongmia.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                                if (co == false)
                                    continue;

                                // Khuyen khich phat trien
                                co = false;
                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", a.Id);

                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                {
                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                    {
                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                        {
                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                            {
                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                if (kkpt1.Id == kkpt2.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                            if (co)
                                            {
                                                //thoat vong for thu 1
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co KKPT trong CTHD
                                {

                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Nhom cu ly
                                co = false;

                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                {
                                    Guid nhomclId = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                        {
                                            if (nhomCL.Id == nhomCL1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomCL trong CTHD
                                {

                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Mo hinh khuyen nong
                                co = false;

                                EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    EntityReference mhknEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                    Guid mhknId = mhknEntityRef.Id;
                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                        {
                                            if (mhkn.Id == mhkn1.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co MNKH trong CTHD
                                {
                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // NHom nang suat
                                co = false;

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                                {
                                                    decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                    decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                    if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                    {
                                                        co = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }
                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                            {
                                                if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                                {
                                                    decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                    decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                    if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                    {
                                                        co = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }

                                if (co == false)
                                    continue;

                                mCSDT = a;
                                break;
                            }
                            if (mCSDTung != null && mCSDTung.Id != Guid.Empty)
                                listCSDT.Add(mCSDTung.ToEntityReference());
                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTung.ToEntityReference() });
                        }

                        //----END---- Tìm CSDT ứng --------------------

                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), listCSDT);
                        #endregion
                    }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    else
                    {
                        throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư trồng chăm sóc mía phù hợp");
                    }
                } // if (vudautuRef == null || vudautuRef.Id == Guid.Empty)
            }//using
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

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        public static EntityCollection FindCSDTtrongmia(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000000));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTthamcanh(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000001));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTtuoi(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000002));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTbocla(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000003));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTung(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000004));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }
        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_chitiethddttrangthietbi' />
                        <attribute name='new_chitiethddtthuedat' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddtmia' operator='eq' uitype='new_thuadatcanhtac' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
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

//Muốn check xem entity này có những field gì và value gì  có thể chạy vòng lặp này:
//foreach (KeyValuePair<String, Object> attribute in entity.Attributes)
//    {
//        Console.WriteLine(attribute.Key + ": " + attribute.Value);
//    }