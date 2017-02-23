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
                Guid entityId = new Guid("78BAC7B5-BCE9-E511-93F6-9ABE942A7E29");


                Entity ChiTietNTTD = service.Retrieve("new_chitietnghiemthuthuedat", entityId, new ColumnSet(true));
                DateTime ngaytao = ChiTietNTTD.GetAttributeValue<DateTime>("createdon");

                EntityReference ThuadatRef = ChiTietNTTD.GetAttributeValue<EntityReference>("new_thuadat");
                Entity thuadatObj = service.Retrieve("new_thuadat", ThuadatRef.Id,
                    new ColumnSet(new string[] {"new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy"}));

                EntityReference NTTDref = ChiTietNTTD.GetAttributeValue<EntityReference>("new_nghiemthuthuedat");
                Entity NTTD = service.Retrieve("new_nghiemthuthuedat", NTTDref.Id,
                    new ColumnSet(new string[] {"new_hopdongdaututhuedat", "new_datthue", "new_lannghiemthu_global"}));

                EntityReference HDDTThuedatRef = NTTD.GetAttributeValue<EntityReference>("new_hopdongdaututhuedat");
                Entity HDDTThuedat = service.Retrieve("new_hopdongthuedat", HDDTThuedatRef.Id,
                    new ColumnSet(new string[] {"new_vudautu"}));

                EntityReference ChiTietHDThueDatRef = NTTD.GetAttributeValue<EntityReference>("new_datthue");
                Entity ChiTietHDThueDat = service.Retrieve("new_datthue", ChiTietHDThueDatRef.Id,
                    new ColumnSet(new string[] {"new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn"}));

                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference) HDDTThuedat["new_vudautu"]).Id,
                    new ColumnSet(new string[] {"new_name"}));
                Entity ctHDTDTD = new Entity("new_chitiethdthuedat_thuadat");

                Entity en = new Entity("new_chitietnghiemthuthuedat");
                en.Id = entityId;

                EntityCollection listCTHDTDthuadat = FindctHDTDTD(service, ChiTietHDThueDat);
                if (listCTHDTDthuadat != null && listCTHDTDthuadat.Entities.Count > 0)
                {
                    string thuadatId = thuadatObj.Id.ToString();

                    foreach (Entity cthdtdtd in listCTHDTDthuadat.Entities)
                    {
                        EntityReference tdref = cthdtdtd.GetAttributeValue<EntityReference>("new_thuadat");
                        Entity td = service.Retrieve("new_thuadat", tdref.Id,
                            new ColumnSet(new string[] {"new_nhomdat"}));
                        string tdId = td.Id.ToString();
                        if (thuadatId == tdId)
                        {
                            ctHDTDTD = service.Retrieve("new_chitiethdthuedat_thuadat", cthdtdtd.Id,
                                new ColumnSet(new string[]
                                {
                                    "createdon", "new_chitiethdthuedat", "new_thuadat", "new_dinhmuc",
                                    "new_chinhsachdautu", "new_sonamthuedat", "new_dientichthucthue"
                                }));
                            break;
                        }
                    }
                } // if(listCTHDTDthuadat != null && listCTHDTDthuadat.Entities.Count > 0)
                if (ctHDTDTD != null)
                {
                    Entity mCSDT = new Entity();
                    EntityCollection CSDTcol = FindCSDTTD(service, ngaytao, Vudautu);
                    if (CSDTcol != null && CSDTcol.Entities.Count > 0)
                    {
                        foreach (Entity a in CSDTcol.Entities)
                        {
                            if (a.Contains("new_nhomdat_vl")) // Nhom dat
                            {
                                if (ChiTietNTTD.Attributes.Contains("new_nhomdat"))
                                {
                                    if (
                                        a["new_nhomdat_vl"].ToString()
                                            .IndexOf(((OptionSetValue) ChiTietNTTD["new_nhomdat"]).Value.ToString()) ==
                                        -1)
                                        continue;
                                }
                            }

                            if (a.Contains("new_sonamthue")) // So nam thue
                            {
                                if (ChiTietNTTD.Contains("new_thoigianthue"))
                                {
                                    int sonamthueCSDT = (int) a["new_sonamthue"];
                                    int sonamthueCTNTTD = (int) ChiTietNTTD["new_thoigianthue"];
                                    if (sonamthueCSDT != sonamthueCTNTTD)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            // NHom khach hang
                            bool co = false;

                            if (ChiTietHDThueDat.Attributes.Contains("new_benchothuedatkh"))
                            {
                                Guid khId =
                                    ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_benchothuedatkh").Id;
                                Entity khObj = service.Retrieve("contact", khId,
                                    new ColumnSet(new string[] {"fullname", "new_nhomkhachhang", "new_nangsuatbinhquan"}));

                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang",
                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang",
                                    new ColumnSet(new string[] {"new_nhomkhachhangid"}), "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    EntityReference nhomkhEntityRef =
                                        khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                    Guid nhomkhId = nhomkhEntityRef.Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId,
                                        new ColumnSet(new string[] {"new_name"}));
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
                                else //neu khong co NHomKH trong CTHD
                                {
                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (ChiTietHDThueDat.Attributes.Contains("new_benchothuedatkhdn"))
                            {
                                Guid khId =
                                    ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_benchothuedatkhdn").Id;
                                Entity khObj = service.Retrieve("account", khId,
                                    new ColumnSet(new string[] {"name", "new_nhomkhachhang"}));

                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang",
                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang",
                                    new ColumnSet(new string[] {"new_nhomkhachhangid"}), "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId,
                                        new ColumnSet(new string[] {"new_name"}));
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
                                else //neu khong co NHomKH trong CTHD
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

                            EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu",
                                "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] {"new_vungid"}),
                                "new_chinhsachdautuid", a.Id);

                            if (thuadatObj.Attributes.Contains("new_vungdialy"))
                            {
                                EntityReference vungdlEntityRef =
                                    thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy");
                                Guid vungdlId = vungdlEntityRef.Id;
                                Entity vungDL = service.Retrieve("new_vung", vungdlId,
                                    new ColumnSet(new string[] {"new_name"}));

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
                            else //neu khong co VungDiaLy trong CTHD
                            {
                                if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;


                            // Nhom cu ly
                            co = false;

                            EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu",
                                "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] {"new_nhomculyid"}),
                                "new_chinhsachdautuid", a.Id);
                            if (thuadatObj.Attributes.Contains("new_nhomculy"))
                            {
                                EntityReference nhomclEntityRef =
                                    thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                Guid nhomclId = nhomclEntityRef.Id;
                                Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId,
                                    new ColumnSet(new string[] {"new_name"}));

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
                            else //neu khong co NHomCL trong CTHD
                            {
                                if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // NHom nang suat
                            co = false;

                            if (ChiTietHDThueDat.Attributes.Contains("new_benchothuedatkh"))
                            {
                                EntityReference khEntityRef =
                                    ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_benchothuedatkh");
                                Guid khId = khEntityRef.Id;
                                Entity khObj = service.Retrieve("contact", khId,
                                    new ColumnSet(new string[] {"fullname", "new_nangsuatbinhquan"}));

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat",
                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat",
                                    new ColumnSet(new string[]
                                        {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                    "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsNhomNS.Entities)
                                        {
                                            if (mhkn1.Attributes.Contains("new_nangsuattu") &&
                                                mhkn1.Attributes.Contains("new_nangsuatden"))
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
                            if (ChiTietHDThueDat.Attributes.Contains("new_benchothuedatkhdn"))
                            {
                                Guid khId =
                                    ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_benchothuedatkhdn").Id;
                                Entity khObj = service.Retrieve("account", khId,
                                    new ColumnSet(new string[] {"name", "new_nangsuatbinhquan"}));

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat",
                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat",
                                    new ColumnSet(new string[]
                                        {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                    "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsNhomNS.Entities)
                                        {
                                            if (mhkn1.Attributes.Contains("new_nangsuattu") &&
                                                mhkn1.Attributes.Contains("new_nangsuatden"))
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
                    }

                    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    {
                        // ------Gan vao Chi tiet PDN giải ngân 
                        EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                        en["new_chinhsachdautu"] = csdtRef;

                        Guid csdtKQ = mCSDT.Id;
                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ,
                            new ColumnSet(new string[]
                            {
                                "new_dinhmucdautuhoanlai", "new_name", "new_muclaisuatdautu",
                                "new_dinhmucdautukhonghoanlai"
                            }));

                        decimal lanNT = NTTD.GetAttributeValue<OptionSetValue>("new_lannghiemthu_global").Value + 2;
                        decimal tyle = 0;
                        // Tim dinh muc dau tu
                        EntityCollection DinhmucDTTMcol = FindDaututienmat(service, csdtKQEntity);
                        if (DinhmucDTTMcol != null && DinhmucDTTMcol.Entities.Count > 0)
                        {
                            foreach (Entity dmdttm in DinhmucDTTMcol.Entities)
                            {
                                decimal ycNT = dmdttm.GetAttributeValue<OptionSetValue>("new_yeucau").Value;
                                if (ycNT == lanNT)
                                {
                                    tyle = (dmdttm.Contains("new_phantramtilegiaingan")
                                        ? (decimal) dmdttm["new_phantramtilegiaingan"]
                                        : 0);
                                    break;
                                }
                            }
                        }

                        decimal tienHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai")
                                             ? ((Money) csdtKQEntity["new_dinhmucdautuhoanlai"]).Value
                                             : 0)*tyle/100;
                        decimal tienKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai")
                                              ? ((Money) csdtKQEntity["new_dinhmucdautukhonghoanlai"]).Value
                                              : 0)*tyle/100;

                        Money MtienHL = new Money(tienHL);

                        en["new_dinhmuc"] = MtienHL;
                        en["new_sotiendautu"] =
                            new Money(tienHL*
                                      (ChiTietNTTD.Contains("new_dientichnghiemthu")
                                          ? (decimal) ChiTietNTTD["new_dientichnghiemthu"]
                                          : 0));

                        service.Update(en);
                    }
                    else
                        throw new InvalidPluginExecutionException("Không có Chính sách Đầu tư thuê đất phù hợp");
                } // if(ctHDTDTD != null)
            } //using
        }

        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2,
            string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id",
                JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id",
                JoinOperator.Inner);

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

        public static EntityCollection FindctHDTDTD(IOrganizationService crmservices, Entity ctHDTD)
        {
            string fetchXml =
                @"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name = 'new_chitiethdthuedat_thuadat' >
                        <attribute name='new_thuadat' />
                        <attribute name = 'new_sotiendaututhucte' />
                        <attribute name='new_sotiendautu' />
                        <attribute name = 'new_sonamthuedat' />
                        <attribute name='new_hientrangcaytrong' />
                        <attribute name = 'new_dientichthuehd' />
                        <attribute name='new_dientichthucthue' />
                        <attribute name = 'new_chitiethdthuedat_thuadatid' />
                        <order attribute='new_thuadat' descending='false' />
                        <filter type = 'and' >
                          <condition attribute='new_chitiethdthuedat' operator='eq' uitype='new_datthue' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, ctHDTD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTTD(IOrganizationService crmservices, DateTime ngaytao, Entity Vudt)
        {
            string fetchXml =
                @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                     <entity name='new_chinhsachdautu'>
                               <attribute name='new_name' />
                               <attribute name='new_vudautu' />
                               <attribute name='new_ngayapdung' />
                               <attribute name='new_mucdichdautu' />
                               <attribute name='new_loaihopdong' />
                               <attribute name='new_dinhmucdautukhonghoanlai' />
                               <attribute name='new_dinhmucdautuhoanlai' />
                               <attribute name='new_sonamthue' />
                               <attribute name='new_chinhsachdautuid' />
                           <order attribute='new_ngayapdung' descending='true' />
                           <filter type='and'>
                               <condition attribute='new_loaihopdong' operator='eq' value='100000001' />
                               <condition attribute='new_mucdichdautu' operator='eq' value='100000005' />
                               <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                               <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                        
                           </filter>
                       </entity>
                     </fetch>";

            fetchXml = string.Format(fetchXml, ngaytao, Vudt.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindDaututienmat(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_dinhmucdautu'>
                    <attribute name='new_name' />
                    <attribute name='new_sotien' />
                    <attribute name='new_langiaingan' />
                    <attribute name='new_phantramtilegiaingan' />
                    <attribute name='new_dinhmucdautuid' />
                    <attribute name='new_yeucau' />
                    <attribute name='new_tyleyc' />
                    <attribute name='new_sotienyc' />
                    <attribute name='new_phanbontoithieuyc' />
                    <attribute name='new_phanbontoithieubt' />
                    <order attribute='new_langiaingan' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, CSDT.Id);
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