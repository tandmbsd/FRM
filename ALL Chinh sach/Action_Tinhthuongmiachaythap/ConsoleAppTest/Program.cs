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

                Guid entityId = new Guid("2D81712C-4EB7-E511-93F1-9ABE942A7E29");

                Entity Bangkechitiencuoivu = service.Retrieve("new_bangkechitiencuoivu", entityId, new ColumnSet(true));
                if (Bangkechitiencuoivu == null)
                    throw new Exception("Bảng kê chi tiền cuối vụ này không tồn tại!");

                if (Bangkechitiencuoivu.Contains("new_vudautu") && Bangkechitiencuoivu.Contains("new_loaibangke"))
                {
                    Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)Bangkechitiencuoivu["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

                    EntityCollection dsNTsauTH = FindNTsauTH(service, Vudautu);

                    DateTime today = DateTime.Now;

                    if (dsNTsauTH != null && dsNTsauTH.Entities.Count > 0)
                    {
                        Entity CSTMcuoivu = FindCSTMcuoivu(service, Vudautu, today);

                        if (CSTMcuoivu != null)
                        {
                            foreach (Entity NTsauTH in dsNTsauTH.Entities)
                            {
                                //DateTime ngaynghiemthu = NTsauTH.GetAttributeValue<DateTime>("actualstart");
                                EntityCollection dsCTNTsauTH = FindCTNTsauTH(service, NTsauTH);
                                if (dsCTNTsauTH != null && dsCTNTsauTH.Entities.Count > 0)
                                {
                                    foreach (Entity CTNTsauTH in dsCTNTsauTH.Entities)
                                    {
                                        decimal tylemiachay = 0;
                                        decimal tylemiachayCSTM = 0;

                                        Entity Thuadat = service.Retrieve("new_thuadat", ((EntityReference)CTNTsauTH["new_thuadat"]).Id, new ColumnSet(new string[] { "new_name" }));
                                        Guid ThuadatID = Thuadat.Id;
                                        bool co = false;

                                        Entity DauCongKH = null;
                                        Entity DauCongKHDN = null;
                                        EntityReference TramRef = null;
                                        EntityReference CBNVRef = null;

                                        EntityCollection dsHDTH = FindHDTH(service, Vudautu);
                                        if (dsHDTH != null && dsHDTH.Entities.Count > 0)
                                        {
                                            foreach (Entity HDthuhoach in dsHDTH.Entities)
                                            {
                                                EntityCollection dsCTHDDTmia = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_hopdongthuhoach", "new_new_hopdongthuhoach_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadat", "new_name" }), "new_hopdongthuhoach", HDthuhoach.Id);

                                                if (dsCTHDDTmia != null && dsCTHDDTmia.Entities.Count > 0)
                                                {
                                                    foreach (Entity CTHDDTmia in dsCTHDDTmia.Entities)
                                                    {
                                                        Entity ThuadatCTHDDTmia = service.Retrieve("new_thuadat", ((EntityReference)CTHDDTmia["new_thuadat"]).Id, new ColumnSet(new string[] { "new_name" }));
                                                        Guid ThuadatCTHDDTmiaID = ThuadatCTHDDTmia.Id;

                                                        if (ThuadatID.CompareTo(ThuadatCTHDDTmiaID) == 0) // 2 thửa đất giống nhau
                                                        {
                                                            if (HDthuhoach.Contains("new_doitacthuhoach"))
                                                                DauCongKH = service.Retrieve("contact", ((EntityReference)HDthuhoach["new_doitacthuhoach"]).Id, new ColumnSet(new string[] { "fullname" }));

                                                            if (HDthuhoach.Contains("new_doitacthuhoachkhdn"))
                                                                DauCongKHDN = service.Retrieve("account", ((EntityReference)HDthuhoach["new_doitacthuhoachkhdn"]).Id, new ColumnSet(new string[] { "name" }));

                                                            if (HDthuhoach.Contains("new_tram"))
                                                                TramRef = HDthuhoach.GetAttributeValue<EntityReference>("new_tram");

                                                            if (HDthuhoach.Contains("new_canbonongvu"))
                                                                CBNVRef = HDthuhoach.GetAttributeValue<EntityReference>("new_canbonongvu");

                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (co == true)
                                                {
                                                    //thoat vong for thu 1
                                                    break;
                                                }

                                            } // foreach (Entity HDthuhoach in dsHDTH.Entities)
                                        } // if (dsHDTH != null && dsHDTH.Entities.Count > 0)

                                        if (co == true)
                                        {
                                            tylemiachay = (CTNTsauTH.Contains("new_tylemiachay") ? (decimal)CTNTsauTH["new_tylemiachay"] : 0);
                                            tylemiachayCSTM = (CSTMcuoivu.Contains("new_phantramtilemiachay") ? (decimal)CSTMcuoivu["new_phantramtilemiachay"] : 0);

                                            if ((tylemiachay > 0) && (tylemiachayCSTM > 0) && (tylemiachay <= tylemiachayCSTM) && (DauCongKH != null || DauCongKHDN != null))
                                            {
                                                if (CSTMcuoivu.Contains("new_hinhthuctinhklthuong"))
                                                {
                                                    decimal sanluongmiachay = 0;
                                                    decimal sanluongmiatuoi = 0;
                                                    decimal khoiluongmia = 0;

                                                    Entity phieuDNthuong = new Entity("new_phieudenghithuong");

                                                    if (DauCongKH != null)
                                                    {
                                                        EntityReference khRef = DauCongKH.ToEntityReference();
                                                        phieuDNthuong.Attributes.Add("new_khachhang", khRef);
                                                    }
                                                    if (DauCongKHDN != null)
                                                    {
                                                        EntityReference khRef = DauCongKHDN.ToEntityReference();
                                                        phieuDNthuong.Attributes.Add("new_khachhangdoanhnghiep", khRef);
                                                    }

                                                    string tenpdn = "Thưởng tỷ lệ mía cháy thấp cho đầu công";
                                                    DateTime ngaylapphieu = DateTime.Now;
                                                    decimal thuongMiachaycstm = (CSTMcuoivu.Contains("new_dinhmucthuongmiachay") ? ((Money)CSTMcuoivu["new_dinhmucthuongmiachay"]).Value : 0);
                                                    Money MthuongMiachaycstm = new Money(thuongMiachaycstm);
                                                    //decimal klmia = 1;

                                                    if (CSTMcuoivu.GetAttributeValue<OptionSetValue>("new_hinhthuctinhklthuong").Value.ToString() == "100000000")
                                                        khoiluongmia = sanluongmiatuoi;
                                                    if (CSTMcuoivu.GetAttributeValue<OptionSetValue>("new_hinhthuctinhklthuong").Value.ToString() == "100000001")
                                                        khoiluongmia = sanluongmiatuoi + sanluongmiachay;

                                                    phieuDNthuong.Attributes.Add("new_name", tenpdn);
                                                    phieuDNthuong.Attributes.Add("new_ngaylapphieu", ngaylapphieu);
                                                    phieuDNthuong.Attributes.Add("new_vudautu", Vudautu.ToEntityReference());
                                                    //phieuDNthuong.Attributes.Add("new_hopdongdautumia", HD.ToEntityReference());
                                                    phieuDNthuong.Attributes.Add("new_loaithuong", new OptionSetValue(100000001));
                                                    phieuDNthuong.Attributes.Add("new_nghiemthusauthuhoach", NTsauTH.ToEntityReference());
                                                    phieuDNthuong.Attributes.Add("new_dinhmucthuong", MthuongMiachaycstm);
                                                    phieuDNthuong.Attributes.Add("new_klmiaduocthuong", khoiluongmia);
                                                    phieuDNthuong.Attributes.Add("new_tram", TramRef);
                                                    phieuDNthuong.Attributes.Add("new_canbonongvu", CBNVRef);

                                                    service.Create(phieuDNthuong);

                                                    EntityReferenceCollection ds = new EntityReferenceCollection();
                                                    ds.Add(phieuDNthuong.ToEntityReference());

                                                    service.Associate("new_bangkechitiencuoivu", Bangkechitiencuoivu.Id, new Relationship("new_new_bangkechitiencuoivu_new_pdnthuong"), ds);
                                                }

                                            } // if((tylemiachay != 0) && (tylemiachayCSTM != 0) && (tylemiachay <= tylemiachayCSTM) && (DauCongKH != null || DauCongKHDN != null))

                                        } // if (co == true)

                                    } // foreach(Entity CTNTsauTH in dsCTNTsauTH.Entities)

                                } // if(dsCTNTsauTH != null && dsCTNTsauTH.Entities.Count > 0)

                            } // foreach (Entity NTsauTH in dsNTsauTH.Entities)

                        } // if(CSTMcuoivu != null)
                        else
                            throw new InvalidPluginExecutionException("Không có chính sách thu mua cuối vụ cho vụ đầu tư này");


                    } // if(dsNTsauTH != null && dsNTsauTH.Entities.Count > 0)
                    else
                        throw new InvalidPluginExecutionException("Không có đầu công nào được thưởng tỷ lệ mía cháy thấp.");

                } // if (Bangkechitiencuoivu.Contains("new_vudautu") && Bangkechitiencuoivu.Contains("new_loaibangke"))
                else
                    throw new InvalidPluginExecutionException("Thiếu thông tin Vụ đầu tư / Loại bảng kê");

                //traceService.Trace("ID là " + newCSDTID.ToString());

            }//using
        }

        public static EntityCollection FindNTsauTH(IOrganizationService crmservices, Entity Vudt)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_nghiemthuchatsatgoc'>
                        <attribute name='subject' />
                        <attribute name='new_tongdientich' />
                        <attribute name='new_manghiemthu' />
                        <attribute name='new_khachhangdoanhnghiep' />
                        <attribute name='new_khachhang' />
                        <attribute name='new_hopdongdautumia' />
                        <attribute name='new_danhgia' />
                        <attribute name='actualstart' />
                        <attribute name='createdon' />
                        <attribute name='activityid' />
                        <order attribute='createdon' descending='true' />
                        <filter type='and'>
                          <condition attribute='new_tinhtrangduyet' operator='eq' value='100000006' />
                        </filter>
                        <link-entity name='new_hopdongdautumia' from='new_hopdongdautumiaid' to='regardingobjectid' alias='ag'>
                          <filter type='and'>
                            <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                          </filter>
                        </link-entity>
                      </entity>
                    </fetch>";

            Guid VudtId = Vudt.Id;
            fetchXml = string.Format(fetchXml, VudtId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;

        }

        public static EntityCollection FindCTNTsauTH(IOrganizationService crmservices, Entity NTsauTH)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietnghiemthusauthuhoach'>
                    <attribute name='new_name' />
                    <attribute name='new_tongsanluong' />
                    <attribute name='new_thuadat' />
                    <attribute name='new_nghiemthusauthuhoach' />
                    <attribute name='new_hopdongthuhoach' />
                    <attribute name='new_miatuoi' />
                    <attribute name='new_miachay' />
                    <attribute name='new_dientich' />
                    <attribute name='new_tylemiachay' />
                    <attribute name='new_chitietnghiemthusauthuhoachid' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_nghiemthusauthuhoach' operator='eq' uitype='new_nghiemthuchatsatgoc' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            Guid NTsauTHId = NTsauTH.Id;
            fetchXml = string.Format(fetchXml, NTsauTHId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;

        }

        public static EntityCollection FindHDTH(IOrganizationService crmservices, Entity Vudt)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_hopdongthuhoach'>
                    <attribute name='new_name' />
                    <attribute name='new_soluongcong' />
                    <attribute name='new_sohopdong' />
                    <attribute name='new_ngaykyhopdong' />
                    <attribute name='new_tram' />
                    <attribute name='new_canbonongvu' />
                    <attribute name='new_khanangthuhoach1ngay' />
                    <attribute name='new_hopdongthuhoachid' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                      <condition attribute='statuscode' operator='eq' value='100000000' />
                    </filter>
                  </entity>
                </fetch>";

            Guid VudtId = Vudt.Id;
            fetchXml = string.Format(fetchXml, VudtId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;

        }
        public static Entity FindCSTMcuoivu(IOrganizationService crmservices, Entity Vudt, DateTime ngaynghiemthu)
        {
            string fetchXml =
                @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chinhsachthumua'>
                    <attribute name='new_name' />
                    <attribute name='new_vudautu' />
                    <attribute name='new_thoidiemapdung' />
                    <attribute name='new_hoatdongapdung' />
                    <attribute name='new_congdieutunoikhac' />
                    <attribute name='new_dongiamiacobantairuong' />
                    <attribute name='new_dongiamiacobantainhamay' />
                    <attribute name='new_machinhsach' />
                    <attribute name='new_chinhsachthumuaid' />
                    <attribute name='new_loaigocmia_vl' />
                    <attribute name='new_nhomdat_vl' />
                    <attribute name='new_vutrong_vl' />
                    <attribute name='new_mucdichsanxuatmia_vl' />
                    <attribute name='new_nhomgiongmia_vl' />
                    <attribute name='new_loaisohuudat_vl' />
                    <attribute name='new_loaimiachay_vl' />
                    <attribute name='new_tinhtrangmia_vl' />
                    <attribute name='new_miachaycoy' />
                    <attribute name='new_thuonghoanthanhhd' />
                    <attribute name='new_thuongchochumia' />
                    <attribute name='new_thuongchatsatgoc' />
                    <attribute name='new_hinhthuctinhklthuongchatsatgoc' /> 
                    <attribute name='new_hinhthuctinhklthuong' /> 
                    <attribute name='new_phantramtilemiachay' />
                    <attribute name='new_dinhmucthuongmiachay' />
                    <attribute name='new_thoidiemketthuc' />
                    <order attribute='new_thoidiemapdung' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_hoatdongapdung' operator='eq' value='100000004' />
                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                      <condition attribute='new_thoidiemapdung' operator='le' value='{1}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, Vudt.Id, ngaynghiemthu);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            if (entc != null && entc.Entities.Count > 0)
            {
                return entc[0];
            }
            else
                return null;
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