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

namespace TrangThietBiConsoleApp
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
                Guid entityId = new Guid("8A173AE0-FAFD-E511-93F7-9ABE942A7E29");

                Entity ChiTietNTTuoiMiaTuoiMia = service.Retrieve("new_chitietnghiemthutuoimia_tuoimia", entityId, new ColumnSet(new string[] { "new_name", "new_chitietnghiemthutuoimia", "new_tuoimia", "new_dongia", "new_dinhmuc", "createdon" }));
                DateTime ngaytao = ChiTietNTTuoiMiaTuoiMia.GetAttributeValue<DateTime>("createdon");

                if (!ChiTietNTTuoiMiaTuoiMia.Contains("new_chitietnghiemthutuoimia"))
                {
                    throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về Chi tiết Nghiệm thu tưới mía");
                }
                else
                {
                    EntityReference TuoiMiaRef = ChiTietNTTuoiMiaTuoiMia.GetAttributeValue<EntityReference>("new_tuoimia");
                    Entity TuoiMia = service.Retrieve("new_tuoimia", TuoiMiaRef.Id, new ColumnSet(new string[] { "subject", "new_hopdongtrongmia", "new_thuacanhtac", "new_phuongphaptuoi", "new_mucdichtuoi", "createdon", "new_dientichthuchien" }));

                    EntityReference ChiTietNTTuoiMiaRef = ChiTietNTTuoiMiaTuoiMia.GetAttributeValue<EntityReference>("new_chitietnghiemthutuoimia");
                    Entity ChiTietNTTuoiMia = service.Retrieve("new_chitietnghiemthutuoimia", ChiTietNTTuoiMiaRef.Id, new ColumnSet(new string[] { "new_nghiemthutuoimia", "new_thuadat" , "new_name" }));

                    Entity NTtuoimiaObj = new Entity();
                    if (ChiTietNTTuoiMia.Contains("new_nghiemthutuoimia"))
                    {
                        EntityReference NTtuoimiaRef = ChiTietNTTuoiMia.GetAttributeValue<EntityReference>("new_nghiemthutuoimia");
                        Guid NTtuoimiaId = NTtuoimiaRef.Id;
                        NTtuoimiaObj = service.Retrieve("new_nghiemthutuoimia", NTtuoimiaId, new ColumnSet(new string[] { "subject", "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongtrongmia", "new_mucdichsanxuatmia" }));
                    }

                    Entity thuadatObj = new Entity();
                    if (ChiTietNTTuoiMia.Contains("new_thuadat"))
                    {
                        EntityReference thuadatEntityRef = ChiTietNTTuoiMia.GetAttributeValue<EntityReference>("new_thuadat");
                        Guid thuadatId = thuadatEntityRef.Id;
                        thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));
                    }

                    Entity HDDTmia = new Entity();
                    if (NTtuoimiaObj != null && NTtuoimiaObj.Contains("new_hopdongtrongmia"))
                    {
                        EntityReference HDDTmiaRef = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_hopdongtrongmia");
                        Guid DHDTmiaId = HDDTmiaRef.Id;
                        HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu" }));
                    }

                    EntityReference vudautuRef = new EntityReference();
                    if (HDDTmia != null && HDDTmia.Contains("new_vudautu"))
                        vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");

                    Guid vuDTId = vudautuRef.Id;

                    //traceService.Trace("vi trí khoi tao");

                    string fetchXml =
                              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_chinhsachdautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_vudautu' />
                                        <attribute name='new_ngayapdung' />
                                        <attribute name='new_mucdichdautu' />
                                        <attribute name='new_loaihopdong' />
                                        <attribute name='new_dinhmucdautuhoanlai' />
                                        <attribute name='new_mucdichtuoi_vl' />
                                        <attribute name='new_phuongphaptuoi_vl' /> 
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_vutrong_vl' />
                                        <attribute name='new_mucdichsanxuatmia_vl' />
                                        <attribute name='new_loaisohuudat_vl' />
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000002' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                      
                                        </filter>
                                      </entity>
                                    </fetch>";

                    fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                    List<Entity> CSDT = result.Entities.ToList<Entity>();

                    //traceService.Trace("vi trí tim CSDT");

                    Entity mCSDT = null;

                    if (CSDT != null && CSDT.Count() > 0)
                    {
                        foreach (Entity a in CSDT)
                        {
                            if (a.Contains("new_mucdichtuoi_vl"))  // Muc dich tuoi
                            {
                                if (TuoiMia.Attributes.Contains("new_mucdichtuoi"))
                                {
                                    if (a["new_mucdichtuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTuoiMia["new_mucdichtuoi"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                {
                                    continue;
                                }
                            }

                            if (a.Contains("new_phuongphaptuoi_vl"))  // Phuong phap tuoi
                            {
                                if (TuoiMia.Attributes.Contains("new_phuongphaptuoi"))
                                {
                                    if (a["new_phuongphaptuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTuoiMia["new_phuongphaptuoi"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                {
                                    continue;
                                }
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

                            //traceService.Trace("vi trí Nhom KH");
                            // NHom khach hang
                            bool co = false;
                            if (NTtuoimiaObj.Attributes.Contains("new_khachhang"))
                            {
                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhang").Id;
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

                            if (NTtuoimiaObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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

                            //traceService.Trace("vi trí truoc NHom nang suat");
                            // NHom nang suat
                            co = false;
                            if (NTtuoimiaObj.Attributes.Contains("new_khachhang"))
                            {
                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                            if (NTtuoimiaObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                    }
                    else
                        throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT Tưới mía nào cho vụ đầu tư này");

                    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    {
                        //traceService.Trace("vi trí Tim ra CSDT" + mCSDT.Id);
                        // ------Gan vao Chi tiet HDDT mia

                        Entity en = new Entity(ChiTietNTTuoiMiaTuoiMia.LogicalName);
                        en.Id = ChiTietNTTuoiMiaTuoiMia.Id;

                        Guid csdtKQ = mCSDT.Id;
                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                        // --------  Gan Dinh muc
                        if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                        {
                            decimal dongia = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautuhoanlai"]).Value : 0);
                            decimal dientich = (TuoiMia.Contains("new_dientichthuchien") ? (decimal)TuoiMia["new_dientichthuchien"] : 0);
                            decimal dinhmuc = dongia * dientich;
                            Money dongiaDT = new Money(dongia);
                            Money dinhmucDT = new Money(dinhmuc);

                            //traceService.Trace("vi trí Dinh muc " + dinhmucDT.Value);

                            en["new_dongia"] = dongiaDT;
                            en["new_dinhmuc"] = dinhmucDT;
                        }
                        // -------- End  Gan Dinh muc

                        EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                        en.Attributes.Add("new_chinhsachdautu", csdtRef);

                        service.Update(en);

                    }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    else
                    {
                        throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư tưới mía phù hợp");
                    }
                }
            }//using
        }
        static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
    }
}
