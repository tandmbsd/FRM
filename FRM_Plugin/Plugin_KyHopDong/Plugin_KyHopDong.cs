using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_KyHopDong
{
    public class Plugin_KyHopDong : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            Entity HD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

            #region
            if (target.LogicalName.Trim().ToLower() == "new_yeucaugiaichap")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000007)
                    {
                        Entity TS = new Entity("new_taisan");
                        TS["new_trangthaitaisan"] = new OptionSetValue(100000000);
                        TS.Id = ((EntityReference)HD["new_taisan"]).Id;
                        service.Update(TS);

                        Entity uHD = new Entity("new_yeucaugiaichap");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongthechap")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        QueryExpression q = new QueryExpression("new_taisanthechap");
                        q.ColumnSet = new ColumnSet(new string[] { "new_taisanthechapid", "new_taisan" });
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("new_hopdongthechap", ConditionOperator.Equal, target.Id));
                        EntityCollection entc = service.RetrieveMultiple(q);

                        foreach (Entity a in entc.Entities)
                        {
                            Entity TS = new Entity("new_taisan");
                            TS.Id = ((EntityReference)a["new_taisan"]).Id;
                            TS["new_trangthaitaisan"] = new OptionSetValue(100000002);
                            service.Update(TS);
                        }

                        Entity uHD = new Entity("new_hopdongthechap");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_chuhopdong"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_chuhopdong"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_chuhopdongdoanhnghiep"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieuthamdinhdautu")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity PTD = new Entity("new_phieuthamdinhdautu");
                        PTD.Id = target.Id;
                        PTD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(PTD);

                        if (HD.Contains("new_hopdongdautumia"))
                        {
                            Entity uHD = new Entity("new_hopdongdautumia");
                            uHD.Id = ((EntityReference)HD["new_hopdongdautumia"]).Id;
                            uHD["new_ngaykyhopdong"] = HD["new_ngayky"];
                            uHD["statuscode"] = new OptionSetValue(100000003);
                            service.Update(uHD);
                        }
                        if (HD.Contains("new_hopdongdaututhuedat"))
                        {
                            Entity uHD = new Entity("new_hopdongthuedat");
                            uHD.Id = ((EntityReference)HD["new_hopdongdaututhuedat"]).Id;
                            uHD["statuscode"] = new OptionSetValue(100000000);
                            uHD["new_ngaykyhopdong"] = HD["new_ngayky"];
                            service.Update(uHD);
                        }

                        if (HD.Contains("new_hopdongthechap"))
                        {
                            Entity uHD = new Entity("new_hopdongthechap");
                            uHD.Id = ((EntityReference)HD["new_hopdongthechap"]).Id;
                            uHD["statuscode"] = new OptionSetValue(100000000);
                            uHD["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                            service.Update(uHD);
                        }

                        if (HD.Contains("new_khachhang"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_khachhang"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_khachhangdoanhnghiep"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongdaututrangthietbi")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongdaututrangthietbi");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_doitaccungcap"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_doitaccungcap"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_doitaccungcapkhdn"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }

            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongthuhoach")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongthuhoach");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_doitacthuhoach"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_doitacthuhoach"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_doitacthuhoachkhdn"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongvanchuyen")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongvanchuyen");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_doitacvanchuyen"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_doitacvanchuyen"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_doitacvanchuyenkhdn"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongcungungdichvu")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongcungungdichvu");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_doitaccungcap"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_doitaccungcap"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_doitaccungcapkhdn"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongmuabanmiangoai")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongmuabanmiangoai");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_khachhang"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_khachhang"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_khachhangdoanhnghiep"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_hopdongdautuhatang")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongdautuhatang");
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);

                        if (HD.Contains("new_doitaccungcap"))
                        {
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_doitaccungcap"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else
                        {
                            Entity KH = new Entity("account");
                            KH.Id = ((EntityReference)HD["new_doitaccungcapkhdn"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                    }
            }
            #endregion
            else if (target.LogicalName.Trim().ToLower() == "new_phieudangkydichvu" ||
               target.LogicalName.Trim().ToLower() == "new_phieudangkyhomgiong" ||
               target.LogicalName.Trim().ToLower() == "new_phieudangkyphanbon" ||
               target.LogicalName.Trim().ToLower() == "new_phieudangkythuoc" ||
               target.LogicalName.Trim().ToLower() == "new_phieudangkyvattu" ||
               target.LogicalName.Trim().ToLower() == "new_danhgianangsuat")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000002)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_nghiemthumaymocthietbi")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieudenghigiaingan")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieutamung")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieudenghithanhtoan")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieudenghithuno")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieuchitienmat") // còn thiếu cập nhật trạng thái các phiếu kèm phiếu chi.
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000003)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000001);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName.Trim().ToLower() == "new_phieudenghithuong")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }

            
	

        }
    }
}
