using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
namespace Plugin_BangChiTienCuoiVu
{
    public class Plugin_BangChiTienCuoiVu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000001)
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bangkechitietcuoivu = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                int loaibangke = ((OptionSetValue)bangkechitietcuoivu["new_loaibangke"]).Value;


                #region thưởng hoàn thành HĐ
                if (loaibangke == 100000000) // thưởng hoàn thành hđ
                {
                    List<Entity> danhsachchitiet = RetrieveNNRecord(service, "new_phieudenghithuong", "new_bangkechitiencuoivu", "new_new_bangkechitiencuoivu_new_pdnthuong", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkechitietcuoivu.Id);
                    Dictionary<string, List<Entity>> dictionary = new Dictionary<string, List<Entity>>();
                    List<Entity> lstHDDTM = new List<Entity>();
                    Entity KH = null;

                    foreach (Entity en in danhsachchitiet)
                    {
                        if (en.Contains("new_khachhang"))
                        {
                            KH = new Entity("contact");
                            KH = service.Retrieve("contact", ((EntityReference)en["new_khachhang"]).Id, new ColumnSet(true));
                        }
                        else if (en.Contains("new_khachhangdoanhnghiep"))
                        {
                            KH = new Entity("account");
                            KH = service.Retrieve("account", ((EntityReference)en["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));
                        }

                        Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)en["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_thuacanhtac" }));
                        string idKhachhang = en.Contains("new_khachhang") ? ((EntityReference)en["new_khachhang"]).Id.ToString() : ((EntityReference)en["new_khachhangdoanhnghiep"]).Id.ToString();
                        string idChitiet = ((EntityReference)lenhdon["new_thuacanhtac"]).Id.ToString();
                        string key = idKhachhang + "," + idChitiet;
                        if (!dictionary.ContainsKey(key))
                        {
                            dictionary.Add(key, new List<Entity>());
                        }
                        dictionary[key].Add(en);
                    }

                    foreach (string key in dictionary.Keys)
                    {
                        foreach (Entity k in dictionary[key])
                        {
                            lstHDDTM = GetHDKhachHang(KH);
                            bool IsFinished = CheckHDHoanThanh(lstHDDTM);
                            decimal klmia = 0;
                            
                            if (IsFinished == true)
                            {                                
                                foreach (Entity en in lstHDDTM)
                                {
                                    klmia = klmia + sumlenhdon(en);
                                    //throw new Exception(klmia.ToString());
                                }
                            }
                            //throw new Exception(klmia.ToString());
                            Entity chitietbangkecuoivu = new Entity("new_chitietbangkechitiencuoivu");
                            chitietbangkecuoivu["new_bangkechitiencuoivu"] = bangkechitietcuoivu.ToEntityReference();
                            Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)k["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_lenhdoncuoi" }));
                            decimal dinhmuc = ((Money)k["new_dinhmucthuong"]).Value;

                            if (k.Contains("new_khachhang"))
                            {
                                chitietbangkecuoivu["new_khachhang"] = k["new_khachhang"];
                            }
                            else if (k.Contains("new_khachhangdoanhnghiep"))
                            {
                                chitietbangkecuoivu["new_khachhangdoanhnghiep"] = k["new_khachhangdoanhnghiep"];
                            }

                            chitietbangkecuoivu["new_hopdongdautumia"] = k["new_hopdongdautumia"];
                            chitietbangkecuoivu["new_dinhmuc"] = new Money(dinhmuc);
                            service.Create(chitietbangkecuoivu);
                            break;
                        }
                    }
                }
                #endregion
                #region thưởng tỉ lệ mía cháy thấp
                else if (loaibangke == 100000001) // thưởng tỉ lệ mía cháy thấp 
                {
                    List<Entity> danhsachchitiet = RetrieveNNRecord(service, "new_phieudenghithuong", "new_bangkechitiencuoivu", "new_new_bangkechitiencuoivu_new_pdnthuong", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkechitietcuoivu.Id);
                    Dictionary<string, List<Entity>> dictionary = new Dictionary<string, List<Entity>>();

                    foreach (Entity en in danhsachchitiet)
                    {
                        Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)en["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_thuacanhtac" }));
                        string idKhachhang = en.Contains("new_khachhang") ? ((EntityReference)en["new_khachhang"]).Id.ToString() : ((EntityReference)en["new_khachhangdoanhnghiep"]).Id.ToString();
                        string idChitiet = ((EntityReference)lenhdon["new_thuacanhtac"]).Id.ToString();
                        string key = idKhachhang + "," + idChitiet;
                        if (!dictionary.ContainsKey(key))
                        {
                            dictionary.Add(key, new List<Entity>());
                        }
                        dictionary[key].Add(en);
                    }

                    foreach (string key in dictionary.Keys)
                    {
                        foreach (Entity k in dictionary[key])
                        {
                            Entity chitietbangkecuoivu = new Entity("new_chitietbangkechitiencuoivu");
                            chitietbangkecuoivu["new_bangkechitiencuoivu"] = bangkechitietcuoivu.ToEntityReference();
                            Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)k["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_lenhdoncuoi" }));
                            decimal dinhmuc = ((Money)k["new_dinhmucthuong"]).Value;
                            decimal klmia = (decimal)k["new_klmiaduocthuong"];

                            if (k.Contains("new_khachhang"))
                            {
                                chitietbangkecuoivu["new_khachhang"] = k["new_khachhang"];
                            }
                            else if (k.Contains("new_khachhangdoanhnghiep"))
                            {
                                chitietbangkecuoivu["new_khachhangdoanhnghiep"] = k["new_khachhangdoanhnghiep"];
                            }

                            chitietbangkecuoivu["new_hopdongthuhoach"] = k["new_hopdongthuhoach"];
                            chitietbangkecuoivu["new_dinhmuc"] = new Money(dinhmuc);
                            chitietbangkecuoivu["new_khoiluongmia"] = k["new_klmiaduocthuong"];
                            chitietbangkecuoivu["new_thanhtien"] = new Money(dinhmuc * klmia);
                            service.Create(chitietbangkecuoivu);
                            break;
                        }
                    }
                }
                #endregion
                #region thưởng chặt sát gốc
                else if (loaibangke == 100000002) // thuong chat sat goc
                {
                    List<Entity> danhsachchitiet = RetrieveNNRecord(service, "new_phieudenghithuong", "new_bangkechitiencuoivu", "new_new_bangkechitiencuoivu_new_pdnthuong", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkechitietcuoivu.Id);
                    Dictionary<string, List<Entity>> dictionary = new Dictionary<string, List<Entity>>();

                    foreach (Entity en in danhsachchitiet)
                    {
                        Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)en["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_thuacanhtac" }));
                        string idKhachhang = en.Contains("new_khachhang") ? ((EntityReference)en["new_khachhang"]).Id.ToString() : ((EntityReference)en["new_khachhangdoanhnghiep"]).Id.ToString();
                        string idChitiet = ((EntityReference)lenhdon["new_thuacanhtac"]).Id.ToString();
                        string key = idKhachhang + "," + idChitiet;
                        if (!dictionary.ContainsKey(key))
                        {
                            dictionary.Add(key, new List<Entity>());
                        }
                        dictionary[key].Add(en);
                    }

                    foreach (string key in dictionary.Keys)
                    {
                        foreach (Entity k in dictionary[key])
                        {
                            Entity chitietbangkecuoivu = new Entity("new_chitietbangkechitiencuoivu");
                            decimal dinhmuc = ((Money)k["new_dinhmucthuong"]).Value;
                            decimal klmia = (decimal)k["new_klmiaduocthuong"];
                            chitietbangkecuoivu["new_bangkechitiencuoivu"] = bangkechitietcuoivu.ToEntityReference();

                            if (k.Contains("new_khachhang"))
                            {
                                chitietbangkecuoivu["new_khachhang"] = k["new_khachhang"];
                            }
                            else if (k.Contains("new_khachhangdoanhnghiep"))
                            {
                                chitietbangkecuoivu["new_khachhangdoanhnghiep"] = k["new_khachhangdoanhnghiep"];
                            }

                            chitietbangkecuoivu["new_hopdongthuhoach"] = k["new_hopdongthuhoach"];
                            chitietbangkecuoivu["new_klmiagiaottcs"] = klmia;
                            chitietbangkecuoivu["new_dongia"] = new Money(dinhmuc);
                            chitietbangkecuoivu["new_thanhtien2"] = new Money(klmia * dinhmuc);
                            service.Create(chitietbangkecuoivu);
                            break;
                        }
                    }
                }
                #endregion
                #region thưởng tạp chất thấp
                else if (loaibangke == 100000003) // thưởng tạp chất thấp
                {

                }
                #endregion
                #region thưởng CSS
                else if (loaibangke == 100000004)
                {
                    List<Entity> danhsachchitiet = RetrieveNNRecord(service, "new_phieudenghithuong", "new_bangkechitiencuoivu", "new_new_bangkechitiencuoivu_new_pdnthuong", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkechitietcuoivu.Id);
                    Dictionary<string, List<Entity>> dictionary = new Dictionary<string, List<Entity>>();

                    foreach (Entity en in danhsachchitiet)
                    {
                        Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)en["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_thuacanhtac" }));
                        string idKhachhang = en.Contains("new_khachhang") ? ((EntityReference)en["new_khachhang"]).Id.ToString() : ((EntityReference)en["new_khachhangdoanhnghiep"]).Id.ToString();
                        string idChitiet = ((EntityReference)lenhdon["new_thuacanhtac"]).Id.ToString();
                        string key = idKhachhang + "," + idChitiet;
                        if (!dictionary.ContainsKey(key))
                        {
                            dictionary.Add(key, new List<Entity>());
                        }
                        dictionary[key].Add(en);
                    }

                    foreach (string key in dictionary.Keys)
                    {
                        foreach (Entity k in dictionary[key])
                        {
                            Entity chitietbangkecuoivu = new Entity("new_chitietbangkechitiencuoivu");
                            decimal dinhmuc = ((Money)k["new_dinhmucthuong"]).Value;
                            decimal klmia = (decimal)k["new_klmiaduocthuong"];
                            chitietbangkecuoivu["new_bangkechitiencuoivu"] = bangkechitietcuoivu.ToEntityReference();

                            if (k.Contains("new_khachhang"))
                            {
                                chitietbangkecuoivu["new_khachhang"] = k["new_khachhang"];
                            }
                            else if (k.Contains("new_khachhangdoanhnghiep"))
                            {
                                chitietbangkecuoivu["new_khachhangdoanhnghiep"] = k["new_khachhangdoanhnghiep"];
                            }

                            chitietbangkecuoivu["new_ngayketthucgiaomia"] = ngayketthucgiaomia(dictionary[key]);
                            chitietbangkecuoivu["new_hopdongthuhoach"] = k["new_hopdongthuhoach"];
                            chitietbangkecuoivu["new_khoiluongmia1"] = klmia;
                            chitietbangkecuoivu["new_dinhmuc1"] = new Money(dinhmuc);
                            chitietbangkecuoivu["new_thanhtien1"] = new Money(klmia * dinhmuc);
                            service.Create(chitietbangkecuoivu);
                            break;
                        }
                    }
                }
                #endregion
                else if (loaibangke == 100000005) // bien ban vi pham
                {
                }
                else if (loaibangke == 100000006) // tam giu
                {

                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        List<Entity> RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityCollection collRecords = service.RetrieveMultiple(query);
            query.AddOrder(entity1 + "id", OrderType.Ascending);

            return collRecords.Entities.ToList<Entity>();
        }
        EntityReferenceCollection RefRetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityCollection collRecords = service.RetrieveMultiple(query);
            EntityReferenceCollection RefcollRecords = new EntityReferenceCollection();

            foreach (Entity en in collRecords.Entities)
            {
                RefcollRecords.Add(en.ToEntityReference());
            }

            return RefcollRecords;
        }

        DateTime ngayketthucgiaomia(List<Entity> pdnt)
        {
            DateTime dt = new DateTime();
            foreach (Entity en in pdnt)
            {
                Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)en["new_lenhdon"]).Id, new ColumnSet(new string[] { "new_ngaycap", "new_lenhdoncuoi" }));

                if (lenhdon.Contains("new_lenhdoncuoi") && (int)lenhdon["new_lenhdoncuoi"] == 1)
                {
                    dt = (DateTime)lenhdon["new_ngaycap"];
                }
            }
            return dt;
        }

        bool CheckHDHoanThanh(List<Entity> lstHDDTM)
        {
            bool flag = true;
            foreach (Entity en in lstHDDTM)
            {
                List<Entity> lstThuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", en.Id);
                foreach (Entity tdct in lstThuadatcanhtac)
                {
                    List<Entity> lstLenhdon = RetrieveMultiRecord(service, "new_lenhdon", new ColumnSet(new string[] { "new_lenhdoncuoi" }), "new_thuacanhtac", tdct.Id);
                    foreach (Entity lenhdon in lstLenhdon)
                    {
                        if (lenhdon["new_lenhdoncuoi"].ToString().ToLower() != "true")
                        {
                            flag = false;
                            break;
                        }
                    }
                }
            }
            return flag;
        }

        List<Entity> GetHDKhachHang(Entity KH)
        {
            Entity vudautuhientai = RetrieveMultiRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true).FirstOrDefault();

            QueryExpression qHddtmhientai = new QueryExpression("new_hopdongdautumia");
            qHddtmhientai.ColumnSet = new ColumnSet(true);
            qHddtmhientai.Criteria = new FilterExpression(LogicalOperator.And);
            qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautuhientai.Id));
            qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, KH.Id));
            qHddtmhientai.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection entcHddtm = service.RetrieveMultiple(qHddtmhientai);
            List<Entity> lstHDDTM = entcHddtm.Entities.ToList<Entity>();
            return lstHDDTM;
        }

        decimal sumlenhdon(Entity hddtm)
        {
            decimal sum = 0; throw new Exception(hddtm["new_name"].ToString());
            StringBuilder fetchXml = new StringBuilder();
            fetchXml.Append("<fetch version='1.0' output-format='xml-platform' mapping='logical' aggregate='true' distinct='false' >");
            fetchXml.Append("<entity name='new_lenhdon'>");
            fetchXml.Append("<attribute name='new_trongluongthanhtoan' alias='sum_trongluongmia' aggregate='sum' />");
            fetchXml.Append("<link-entity name='new_thuadatcanhtac' from='new_thuadatcanhtacid' to='new_thuacanhtac' link-type='inner'>");
            fetchXml.Append("<filter type='and'>");
            fetchXml.Append(string.Format("<condition attribute='new_hopdongdautumia' operator='eq' value ='{0}'></condition>", hddtm.Id));
            fetchXml.Append("</filter>");
            fetchXml.Append("</link-entity>");
            fetchXml.Append("</entity>");
            fetchXml.Append("</fetch>");

            EntityCollection etns = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
            
            foreach (var c in etns.Entities)
            {
                if (!c.Attributes.ContainsKey("sum_trongluongmia"))
                {
                    sum = sum + ((Decimal)((AliasedValue)c["sum_trongluongmia"]).Value); throw new Exception(sum.ToString());
                }
            }
            
            return sum;
        }
    }
}
