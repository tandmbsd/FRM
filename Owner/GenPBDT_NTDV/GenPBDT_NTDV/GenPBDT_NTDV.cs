using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Messaging;

namespace GenPBDT_NTDV
{
    public class GenPBDT_NTDV : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            int type;

            if (target.Contains("new_tinhtrangduyet") && ((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006) // da duyet
            {
                trace.Trace("Start");

                Entity ntdichvu = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(true));

                EntityReference hdmiaRef = (EntityReference)ntdichvu["new_hopdongdautumia"];
                Entity hddtmia = service.Retrieve(hdmiaRef.LogicalName, hdmiaRef.Id,
                    new ColumnSet(new string[] { "new_masohopdong", "new_vudautu" }));
                EntityReference tram = null;
                EntityReference cbnv = null;
                DateTime ngayduyet = new DateTime();
                EntityReference vudautu = null;
                string sophieu = (string)ntdichvu["new_manghiemthu"];

                Entity KH = null;

                if (ntdichvu.Contains("new_khachhangdautumia"))
                    KH = service.Retrieve("contact", ((EntityReference)ntdichvu["new_khachhangdautumia"]).Id,
                        new ColumnSet(new string[] { "fullname" }));
                else if (ntdichvu.Contains("new_khachhangdoanhnghiepdautumia"))
                    KH = service.Retrieve("account", ((EntityReference)ntdichvu["new_khachhangdoanhnghiepdautumia"]).Id,
                        new ColumnSet(new string[] { "name" }));

                if (ntdichvu.Contains("new_tram"))
                    tram = (EntityReference)ntdichvu["new_tram"];
                if (ntdichvu.Contains("new_canbonongvu"))
                    cbnv = (EntityReference)ntdichvu["new_canbonongvu"];

                if (ntdichvu.Contains("actualstart"))
                    ngayduyet = (DateTime)ntdichvu["actualstart"];

                if (hddtmia.Contains("new_vudautu"))
                    vudautu = (EntityReference)hddtmia["new_vudautu"];

                List<Entity> lstChitietntdichvu = RetrieveMultiRecord(service, "new_chitietnghiemthudichvu",
                    new ColumnSet(new string[] { "new_thuadat", "new_sotienhl", "new_sotienkhl" }), "new_nghiemthudichvu", ntdichvu.Id);

                foreach (Entity en in lstChitietntdichvu)
                {
                    EntityReference thuadat = (EntityReference)en["new_thuadat"];
                    Entity chitiet = GetThuadatcanhtacfromthuadat(thuadat, hdmiaRef);
                    decimal sotienkhl = en.Contains("new_sotienkhl") ? ((Money)en["new_sotienkhl"]).Value : 0;

                    Entity CSDT = service.Retrieve("new_chinhsachdautu", ((EntityReference)chitiet["new_chinhsachdautu"]).Id,
                        new ColumnSet(new string[] { "new_new_thoihanthuhoivon_khl", "new_machinhsach" }));

                    int sonamthuhoiKHL = CSDT.Contains("new_new_thoihanthuhoivon_khl") ? (int)CSDT["new_new_thoihanthuhoivon_khl"] : 3;
                    decimal sotienphanboKHL = 0;

                    if (sonamthuhoiKHL != 0)
                        sotienphanboKHL = sotienkhl / sonamthuhoiKHL;
                    else
                        return;

                    List<Entity> lst = RetrieveVudautu().Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).
                        ToList<Entity>();
                    int curr = lst.FindIndex(p => p.Id == vudautu.Id);

                    for (int k = 0; k < sonamthuhoiKHL; k++)
                    {
                        Entity vudaututhuhoi = lst[++curr];

                        CreatePBDT(hddtmia, KH, chitiet.Id, vudautu, vudaututhuhoi.ToEntityReference(), sotienphanboKHL,
                                    tram, cbnv, ngayduyet, ntdichvu, sophieu, type = 1);

                        if (curr > lst.Count - 1)
                            throw new Exception("Phân bổ không hoàn lại thất bại. Vui lòng tạo thêm vụ đầu tư mới để phân bổ");
                    }
                }

                foreach (Entity en in lstChitietntdichvu)
                {
                    EntityReference thuadat = (EntityReference)en["new_thuadat"];
                    Entity thuadatcanhtac = GetThuadatcanhtacfromthuadat(thuadat, hdmiaRef);

                    decimal sotien = en.Contains("new_sotienhl") ? ((Money)en["new_sotienhl"]).Value : 0;
                    List<Entity> lstTylethuhoi = RetrieveMultiRecord(service, "new_tylethuhoivondukien",
                    new ColumnSet(new string[] { "new_sotienthuhoi", "new_tiendaphanbo", "new_vudautu", "new_tylephantram" }), "new_chitiethddtmia", thuadatcanhtac.Id);

                    foreach (Entity tylethuhoivon in lstTylethuhoi)
                    {
                        EntityReference vuthuhoi = (EntityReference)tylethuhoivon["new_vudautu"];

                        decimal dinhmuc = sotien * (decimal)tylethuhoivon["new_tylephantram"] / 100;
                        decimal tiendaphanbo = tylethuhoivon.Contains("new_tiendaphanbo") ?
                             ((Money)tylethuhoivon["new_tiendaphanbo"]).Value : new decimal(0);

                        decimal sotienthuhoi = tylethuhoivon.Contains("new_sotienthuhoi") ?
                            ((Money)tylethuhoivon["new_sotienthuhoi"]).Value : 0;

                        decimal sotienphanbo = sotienthuhoi - tiendaphanbo;

                        trace.Trace("chi tiet : " + thuadatcanhtac["new_name"].ToString());
                        trace.Trace("so tien thu hoi : " + sotienthuhoi.ToString());
                        trace.Trace("so tien phan bo : " + sotienphanbo.ToString());
                        trace.Trace("dinh muc : " + dinhmuc.ToString());
                        while (sotienphanbo > 0)
                        {
                            if (dinhmuc <= sotienphanbo)
                            {
                                trace.Trace("phan bo:" + dinhmuc.ToString());
                                CreatePBDT(hddtmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, dinhmuc,
                                    tram, cbnv, ngayduyet, ntdichvu, sophieu, type = 2);
                                tiendaphanbo = tiendaphanbo + dinhmuc;

                                break;
                            }
                            else if (dinhmuc > sotienphanbo)
                            {
                                trace.Trace("phan bo:" + sotienphanbo.ToString());
                                CreatePBDT(hddtmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, sotienphanbo,
                                    tram, cbnv, ngayduyet, ntdichvu, sophieu, type = 2);
                                tiendaphanbo = tiendaphanbo + sotienphanbo;
                                trace.Trace("tien da phan bo:" + tiendaphanbo.ToString());
                                dinhmuc = dinhmuc - sotienphanbo;
                                trace.Trace("dinh muc:" + dinhmuc.ToString());

                            }

                            tylethuhoivon["new_tiendaphanbo"] = new Money(tiendaphanbo);
                            //service.Update(tilethuhoivon);
                        }
                    }
                }
            }
        }
        public void CreatePBDT(Entity hddtmia, Entity KH, Guid tdct,
           EntityReference vudautu, EntityReference vuthanhtoan, decimal sotien, EntityReference tram,
           EntityReference cbnv, DateTime ngayduyet, Entity ntdichvu, string sophieu, int type)
        {
            Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", tdct,
                new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat" }));
            trace.Trace("start phan bo");
            int loailaisuat = ((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value;

            // type = 1 - khl , type = 2 - hl
            if (sotien > 0)
            {
                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");

                if (hddtmia.Contains("new_masohopdong"))
                    Name.Append("-" + hddtmia["new_masohopdong"].ToString());

                if (KH.Contains("fullname"))
                    Name.Append("-" + KH["fullname"]);

                else if (KH.Contains("name"))
                    Name.Append("-" + KH["name"]);

                #region phan bo KHL
                Entity phanbodautuKHL = new Entity("new_phanbodautu");

                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                trace.Trace("type : " + type.ToString());
                if (type == 2)
                {
                    Entity a = service.Retrieve("new_thuadatcanhtac", tdct,
                        new ColumnSet(new string[] { "new_dachihoanlai_dichvu", "new_name" }));
                    decimal dachihoanlai = a.Contains("new_dachihoanlai_dichvu") ? ((Money)a["new_dachihoanlai_dichvu"]).Value : new decimal(0);
                    
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit
                    a["new_dachihoanlai_dichvu"] = new Money(sotien + dachihoanlai);
                    
                    trace.Trace(a["new_name"].ToString());
                    trace.Trace((sotien).ToString());
                    trace.Trace((dachihoanlai).ToString());

                    service.Update(a);
                    
                }
                else if (type == 1)
                {
                    Entity a = service.Retrieve("new_thuadatcanhtac", tdct, new ColumnSet(new string[] { "new_dachikhonghoanlai_dichvu" }));
                    decimal dachikhonghoanlai = a.Contains("new_dachikhonghoanlai_dichvu") ? ((Money)a["new_dachikhonghoanlai_dichvu"]).Value : new decimal(0);

                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000002); // standard
                    a["new_dachikhonghoanlai_dichvu"] = new Money(sotien + dachikhonghoanlai);
                    trace.Trace((sotien + dachikhonghoanlai).ToString());
                    service.Update(a);
                }

                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac", tdct);
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = vuthanhtoan;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram;
                phanbodautuKHL["new_cbnv"] = cbnv;
                phanbodautuKHL["new_ngayphatsinh"] = ngayduyet;
                phanbodautuKHL["new_nghiemthudichvu"] = ntdichvu.ToEntityReference();
                phanbodautuKHL["new_loailaisuat"] = new OptionSetValue(loailaisuat);
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngayduyet);
                trace.Trace("end phan bo");

                service.Create(phanbodautuKHL);
                #endregion
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

        private void Send(Entity tmp)
        {
            MessageQueue mq;

            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle"))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle");
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle");

            Message m = new Message();
            if (tmp != null)
            {
                m.Body = Serialize(tmp);
                m.Label = "invo";
            }
            else
                m.Label = "brek";
            mq.Send(m);
        }

        private List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        private decimal Getlaisuat(EntityReference vudautu, int mucdichdautu, DateTime ngaygiaonhan)
        {
            trace.Trace("get lai suat");
            QueryExpression qbangLai = new QueryExpression("new_banglaisuatthaydoi");
            qbangLai.ColumnSet = new ColumnSet(new string[] { "new_name", "new_ngayapdung", "new_phantramlaisuat" });
            qbangLai.Criteria = new FilterExpression(LogicalOperator.And);
            //qbangLai.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal,
            //    ((EntityReference)thuacanhtac["new_chinhsachdautu"]).Id));
            qbangLai.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qbangLai.Criteria.AddCondition(new ConditionExpression("new_vudautuapdung", ConditionOperator.Equal, vudautu.Id));
            qbangLai.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, mucdichdautu));
            qbangLai.AddOrder("new_ngayapdung", OrderType.Ascending);
            EntityCollection bls = service.RetrieveMultiple(qbangLai);
            //Entity kq = null;
            decimal result = 0;
            int n = bls.Entities.Count;
            trace.Trace("so bang lai " + n.ToString());
            for (int i = 0; i < n; i++)
            {
                Entity q = bls[i];

                DateTime dt = (DateTime)q["new_ngayapdung"];
                if (n == 1 && CompareDate(ngaygiaonhan, dt) == 0)
                {
                    trace.Trace("A1");
                    result = (decimal)q["new_phantramlaisuat"];
                    break;
                }
                else if (n > 1 && CompareDate(ngaygiaonhan, dt) < 0)
                {
                    trace.Trace("B1");
                    result = (decimal)bls[i - 1]["new_phantramlaisuat"];
                    break;
                }
                else if (i == n - 1)
                {
                    trace.Trace("C1");
                    result = (decimal)bls[(i > 0 ? i : 1) - 1]["new_phantramlaisuat"];
                    break;
                }
            }

            return result;
        }

        private decimal CompareDate(DateTime date1, DateTime date2) // begin,end
        {
            string currentTimerZone = TimeZoneInfo.Local.Id;
            DateTime d1 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date1, currentTimerZone);
            DateTime d2 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date2, currentTimerZone);
            decimal temp = (decimal)d1.Date.Subtract(d2.Date).TotalDays;
            return temp;
        }

        private EntityReference GetVuthanhtoan(EntityReference vudautu)
        {
            QueryExpression q = new QueryExpression(vudautu.LogicalName);
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection a = service.RetrieveMultiple(q);

            List<Entity> lst = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == vudautu.Id);

            return lst[curr + 1].ToEntityReference();
        }

        private Entity GetThuadatcanhtacfromthuadat(EntityReference thuadat, EntityReference hdmia)
        {
            Entity rs = null;

            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_name", "new_chinhsachdautu" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, thuadat.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hdmia.Id));
            q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            EntityCollection entc = service.RetrieveMultiple(q);
            if (entc.Entities.Count > 0)
                rs = entc.Entities[0];

            return rs;
        }

        EntityCollection RetrieveVudautu()
        {
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);

            EntityCollection entc = service.RetrieveMultiple(q);

            return entc;
        }
    }
}
