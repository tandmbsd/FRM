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
using System.ServiceModel.Description;

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //OrganizationService service;
            //var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            //var serverConnection = new ServerConnection(connectionstring);

            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"dev2\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.1.93/TEST/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;


            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            //using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                IOrganizationService service = (IOrganizationService)serviceProxy;

                Guid entityId = new Guid("34B8D178-A837-E611-93EF-98BE942A7E2D");

                Entity CTNTTuoiMia = service.Retrieve("new_chitietnghiemthutuoimia", entityId, new ColumnSet(new string[] { "new_nghiemthutuoimia", "new_thuadat", "new_mucdichtuoi", "new_dientichtuoi", "new_dttuoidatyeucau", "createdon" }));
                DateTime ngaytao = CTNTTuoiMia.GetAttributeValue<DateTime>("createdon");
                Entity ChitietHDMia;

                //if ((target.Contains("new_thuadat") || target.Contains("new_mucdichtuoi")) && CTNTTuoiMia.Contains("new_nghiemthutuoimia"))
                if (CTNTTuoiMia.Contains("new_thuadat") && CTNTTuoiMia.Contains("new_nghiemthutuoimia"))
                {

                    //del all new_chitietnghiemthutuoimia_tuoimia cũ
                    QueryExpression qexoa = new QueryExpression("new_chitietnghiemthutuoimia_tuoimia");
                    qexoa.ColumnSet = new ColumnSet(new string[] { "new_chitietnghiemthutuoimia_tuoimiaid" });
                    qexoa.Criteria.AddCondition(new ConditionExpression("new_chitietnghiemthutuoimia", ConditionOperator.Equal, entityId));
                    foreach (Entity a in service.RetrieveMultiple(qexoa).Entities)
                        service.Delete(a.LogicalName, a.Id);

                    //get Chi tiết hợp đồng mía
                    QueryExpression qe = new QueryExpression("new_thuadatcanhtac");
                    qe.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid" });

                    var linktoHDdautumia = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                    var linktoNTTuoimia = new LinkEntity("new_hopdongdautumia", "new_nghiemthutuoimia", "new_hopdongdautumiaid", "new_hopdongtrongmia", JoinOperator.Inner);
                    linktoNTTuoimia.LinkCriteria.AddCondition(new ConditionExpression("activityid", ConditionOperator.Equal, ((EntityReference)CTNTTuoiMia["new_nghiemthutuoimia"]).Id));
                    linktoHDdautumia.LinkEntities.Add(linktoNTTuoimia);
                    qe.LinkEntities.Add(linktoHDdautumia);
                    qe.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)CTNTTuoiMia["new_thuadat"]).Id));

                    EntityCollection result = service.RetrieveMultiple(qe);

                    //traceService.Trace("Lay duoc chi tiet HDDT mia " + result.Entities.Count());
                    //throw new Exception("chay plugin tim CSDT 2");

                    if (result.Entities.Count > 0)
                    {
                        ChitietHDMia = result[0];
                        //throw new Exception("chay plugin tim CSDT 3");

                        //get all active tưới mía and create.
                        QueryExpression allTuoi = new QueryExpression("new_tuoimia");
                        allTuoi.ColumnSet = new ColumnSet(new string[] { "activityid", "subject" });
                        allTuoi.Criteria.AddCondition(new ConditionExpression("new_thuacanhtac", ConditionOperator.Equal, result[0].Id));
                        allTuoi.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 1));

                        EntityCollection dsTuoi = service.RetrieveMultiple(allTuoi);
                        if (dsTuoi.Entities.Count > 0)
                            foreach (Entity a in dsTuoi.Entities)
                            {
                                Entity l = new Entity("new_chitietnghiemthutuoimia_tuoimia");
                                l["new_chitietnghiemthutuoimia"] = new EntityReference("new_chitietnghiemthutuoimia", entityId);
                                l["new_tuoimia"] = new EntityReference("new_tuoimia", a.Id);
                                l["new_name"] = a.Contains("subject") ? a["subject"].ToString() : "No name";
                                service.Create(l);
                            }

                        //set data to update Chi tiet nghiem thu tuoi mia

                        Entity upCTNTTuoi = new Entity("new_chitietnghiemthutuoimia");
                        upCTNTTuoi.Id = entityId;
                        upCTNTTuoi["new_solantuoi"] = result.Entities.Count;
                        upCTNTTuoi["new_mucdichtuoi"] = new OptionSetValue(result.Entities.Count <= 2 ? 100000000 : 100000001);

                        service.Update(upCTNTTuoi);

                        //Get chính sách and set data CTNT Tưới mía 
                        //................
                        #region begin set chính sách

                        Entity NTtuoimiaObj = new Entity();
                        if (CTNTTuoiMia.Contains("new_nghiemthutuoimia"))
                        {
                            EntityReference NTtuoimiaRef = CTNTTuoiMia.GetAttributeValue<EntityReference>("new_nghiemthutuoimia");
                            Guid NTtuoimiaId = NTtuoimiaRef.Id;
                            NTtuoimiaObj = service.Retrieve("new_nghiemthutuoimia", NTtuoimiaId, new ColumnSet(new string[] { "subject", "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongtrongmia", "new_mucdichsanxuatmia" }));
                        }

                        Entity thuadatObj = new Entity();
                        if (CTNTTuoiMia.Contains("new_thuadat"))
                        {
                            EntityReference thuadatEntityRef = CTNTTuoiMia.GetAttributeValue<EntityReference>("new_thuadat");
                            Guid thuadatId = thuadatEntityRef.Id;
                            thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi" }));
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

                        Guid vuDTId = new Guid();
                        if (vudautuRef != null)
                            vuDTId = vudautuRef.Id;

                        //traceService.Trace("vi trí khoi tao");

                        decimal solantuoi = result.Entities.Count;
                        string mucdichtuoi = (solantuoi <= 2 ? "100000000" : "100000001");

                        if (vuDTId != null)
                        {
                            EntityCollection dsCSDT = FindCSDTTuoimia(service, ngaytao, vuDTId);

                            //traceService.Trace("vi trí ds CSDT tuoi " + dsCSDT.Entities.Count());

                            Entity mCSDT = null;

                            if (dsCSDT != null && dsCSDT.Entities.Count() > 0)
                            {
                                foreach (Entity a in dsCSDT.Entities)
                                {
                                    if (a.Contains("new_mucdichtuoi_vl"))  // Muc dich tuoi
                                    {
                                        if (CTNTTuoiMia.Attributes.Contains("new_mucdichtuoi"))
                                        {
                                            if (a["new_mucdichtuoi_vl"].ToString().IndexOf(mucdichtuoi) == -1)
                                                continue;
                                        }
                                        else
                                            continue;
                                    }

                                    //traceService.Trace("Pass Muc dich tuoi");

                                    //if (a.Contains("new_phuongphaptuoi_vl"))  // Phuong phap tuoi
                                    //{
                                    //    if (CTNTTuoiMia.Attributes.Contains("new_phuongphaptuoi"))
                                    //    {
                                    //        if (a["new_phuongphaptuoi_vl"].ToString().IndexOf(((OptionSetValue)CTNTTuoiMia["new_phuongphaptuoi"]).Value.ToString()) == -1)
                                    //            continue;
                                    //    }
                                    //    else
                                    //    {
                                    //        continue;
                                    //    }
                                    //}

                                    if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                    {
                                        if (CTNTTuoiMia.Contains("new_mucdichsanxuatmia"))
                                        {
                                            if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)CTNTTuoiMia["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                                continue;
                                        }
                                        else
                                            continue;
                                    }

                                    //traceService.Trace("Pass Muc dich sx mia");

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

                                    //traceService.Trace("Pass Nhom dat");
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

                                   // traceService.Trace("Pass Nhom KH");
                                    //Vung dia ly
                                    EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                    if (dsVungDL.Entities.Count > 0)
                                    {
                                        co = false;

                                        List<Guid> dsvung = new List<Guid>();
                                        foreach (Entity n in dsVungDL.Entities)
                                            dsvung.Add(n.Id);

                                        //traceService.Trace("So luong Vung DL " + dsVungDL.Entities.Count());

                                        if (thuadatObj.Attributes.Contains("new_diachi"))
                                        {
                                            //traceService.Trace("neu thua dat co dia chi ");
                                            Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadatObj["new_diachi"]).Id, new ColumnSet(new string[] { "new_path" }));
                                           // traceService.Trace("Lay new_path dia chi " + diachi["new_path"].ToString());

                                            QueryExpression qeVDL = new QueryExpression("new_vungdialy_hanhchinh");
                                            qeVDL.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                            qeVDL.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));

                                            //traceService.Trace("Query duoc vung dia ly hanh chinh " + service.RetrieveMultiple(qe).Entities.Count);

                                            if (service.RetrieveMultiple(qe).Entities.Count > 0)
                                            {
                                                //traceService.Trace("So luong Vung dl hanh chinh lon hon 0 ");
                                                foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                                {
                                                    if (n.Contains("new_path") && n["new_path"] != null)
                                                    {
                                                        //traceService.Trace("n co new_path ");
                                                        if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                                        {
                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                    else
                                                    {
                                                    
                                                        co = false;
                                                    }
                                                    
                                                }
                                               
                                            }
                                            
                                        }
                                     
                                        if (co == false)
                                            continue;
                
                                    }

                                   // traceService.Trace("Pass Vung DL");

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

                                    //traceService.Trace("Pass Nhom cu ly");
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

                                    //traceService.Trace("Pass Nhom nang suat");

                                    mCSDT = a;
                                    break;
                                }
                            }
                            else
                                throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT Tưới mía nào cho vụ đầu tư này");

                            //traceService.Trace("vi trí Tim CSDT " + mCSDT.Id);
                            //throw new Exception("vi trí Tim CSDT");

                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                //traceService.Trace("vi trí Tim ra CSDT " + mCSDT.Id);
                                // ------Gan CSDT vao CT Nghiem thu Tuoi mia

                                Guid csdtKQ = mCSDT.Id;
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                Entity en = new Entity("new_chitietnghiemthutuoimia");
                                en.Id = entityId;

                                en["new_chinhsachdautu"] = csdtKQEntity.ToEntityReference();

                                // --------  Gan Dinh muc

                                decimal dinhmucHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautuhoanlai"]).Value : 0);
                                decimal dinhmucKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);

                                decimal dientich = (CTNTTuoiMia.Contains("new_dttuoidatyeucau") ? (decimal)CTNTTuoiMia["new_dttuoidatyeucau"] : 0);

                                decimal thanhtienHL = dinhmucHL * dientich;
                                decimal thanhtienKHL = dinhmucKHL * dientich;

                                //traceService.Trace("vi trí Dinh muc HL" + dinhmucHL);

                                en["new_dongiadinhmuchl"] = new Money(dinhmucHL);
                                //traceService.Trace("cap nhat xong Don gia HL");
                                en["new_dongiadinhmuckhl"] = new Money(dinhmucKHL);

                                //traceService.Trace("cap nhat xong Don gia KHL");

                                en["new_thanhtiendmhl"] = new Money(thanhtienHL);
                                en["new_thanhtiendmkhl"] = new Money(thanhtienKHL);

                                //traceService.Trace("cap nhat xong thanh tien");

                                decimal daGNHL = 0;
                                decimal daGNKHL = 0;

                                EntityCollection dsCTNTtuoimia = FindctNTTuoimia(service, thuadatObj);
                                if (dsCTNTtuoimia != null && dsCTNTtuoimia.Entities.Count() > 0)
                                {
                                    foreach (Entity CTNTTM in dsCTNTtuoimia.Entities)
                                    {
                                        Entity NghiemthuTM = new Entity();
                                        if (CTNTTM.Contains("new_nghiemthutuoimia"))
                                        {
                                            //traceService.Trace("new_nghiemthutuoimia");
                                            EntityReference NTTMRef = CTNTTM.GetAttributeValue<EntityReference>("new_nghiemthutuoimia");

                                            //traceService.Trace("NTTMRef");

                                            NghiemthuTM = service.Retrieve("new_nghiemthutuoimia", NTTMRef.Id, new ColumnSet(new string[] { "statuscode" }));

                                            //traceService.Trace("NghiemthuTM");

                                            string tinhtrangNT = ((OptionSetValue)NghiemthuTM["statuscode"]).Value.ToString();

                                           // traceService.Trace("tinhtrangNT");

                                            if (tinhtrangNT == "100000000")  // NT  da duyet
                                            {
                                                daGNHL += (NghiemthuTM.Contains("new_dautuhl") ? NghiemthuTM.GetAttributeValue<Money>("new_dautuhl").Value : 0);
                                                //traceService.Trace("daGNHL");
                                                daGNKHL += (NghiemthuTM.Contains("new_dautukhl") ? NghiemthuTM.GetAttributeValue<Money>("new_dautukhl").Value : 0);

                                                //traceService.Trace("daGNKHL");
                                            }
                                        }
                                    }
                                }

                                en["new_dagn_hoanlai"] = new Money(daGNHL);

                                //traceService.Trace("new_dagn_hoanlai");

                                en["new_dagn_khl"] = new Money(daGNKHL);

                                //traceService.Trace("new_dagn_khl");

                                decimal denghiHL = thanhtienHL - daGNHL;
                                decimal denghiKHL = thanhtienKHL - daGNKHL;
                                decimal tongtiendt = denghiHL + denghiKHL;

                                if (denghiHL >= 0)
                                    en["new_dautuhl"] = new Money(denghiHL);

                               // traceService.Trace("new_dautuhl");

                                if (denghiKHL >= 0)
                                    en["new_dautukhl"] = new Money(denghiKHL);

                                //traceService.Trace("new_dautukhl");

                                if (tongtiendt >= 0)
                                    en["new_tongtiendautu"] = new Money(tongtiendt);

                                //traceService.Trace("new_tongtiendautu");

                                service.Update(en);

                                // -------- End  Gan Dinh muc

                            }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            else
                            {
                                //traceService.Trace("else  - Không tìm thấy Chính sách Đầu tư tưới mía phù hợp");
                                //throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư tưới mía phù hợp");
                            }
                        } // if(vuDTId != null)

                        #endregion

                    }
                    else
                        throw new InvalidPluginExecutionException("Thửa đất này không có chi tiết HĐĐT mía nào");
                }
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

        public static EntityCollection FindCSDTTuoimia(IOrganizationService crmservices, DateTime ngaytao, Guid VuDTid)
        {
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

            fetchXml = string.Format(fetchXml, ngaytao, VuDTid);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
        public static EntityCollection FindctNTTuoimia(IOrganizationService crmservices, Entity Thuadat)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chitietnghiemthutuoimia'>
                        <attribute name='new_chitietnghiemthutuoimiaid' />
                        <attribute name='new_name' />
                        <attribute name='new_nghiemthutuoimia' />
                        <attribute name='new_thuadat' />
                        <attribute name='new_dautuhl' />
                        <attribute name='new_dautukhl' />
                        <attribute name='new_tongtiendautu' />
                        <attribute name='createdon' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_thuadat' operator='eq' uitype='new_thuadat' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, Thuadat.Id);
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