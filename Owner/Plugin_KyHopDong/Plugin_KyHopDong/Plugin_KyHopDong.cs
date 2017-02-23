using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime;
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
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.Depth > 1)
                return;

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
                        //QueryExpression q = new QueryExpression("new_taisanthechap");
                        //q.ColumnSet = new ColumnSet(new string[] { "new_taisanthechapid", "new_taisan" });
                        //q.Criteria = new FilterExpression();
                        //q.Criteria.AddCondition(new ConditionExpression("new_hopdongthechap", ConditionOperator.Equal, target.Id));
                        //EntityCollection entc = service.RetrieveMultiple(q);

                        //foreach (Entity a in entc.Entities)
                        //{
                        //    Entity TS = new Entity("new_taisan");
                        //    TS.Id = ((EntityReference)a["new_taisan"]).Id;
                        //    TS["new_trangthaitaisan"] = new OptionSetValue(100000002);
                        //    service.Update(TS);
                        //}

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

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykihopdong" }));

                        if (!upHD.Contains("new_ngaykihopdong"))
                        {
                            upHD["new_ngaykihopdong"] = DateTime.Now;
                            service.Update(upHD);
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

                        // Cập nhật Trạng thái đề nghị đầu tư = Hoàn tất
                        if (HD.Contains("new_denghidautu"))
                        {
                            Entity uHD = new Entity("opportunity");
                            uHD.Id = ((EntityReference)HD["new_denghidautu"]).Id;
                            uHD["statuscode"] = new OptionSetValue(100000006);
                            service.Update(uHD);
                        }

                        if (HD.Contains("new_hopdongdautumia"))
                        {
                            Entity uHD = new Entity("new_hopdongdautumia");
                            uHD.Id = ((EntityReference)HD["new_hopdongdautumia"]).Id;

                            uHD = service.Retrieve("new_hopdongdautumia", ((EntityReference)HD["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "statuscode" }));

                            if (uHD.Contains("statuscode") && (((OptionSetValue)uHD["statuscode"]).Value.ToString() == "1" || ((OptionSetValue)uHD["statuscode"]).Value.ToString() == "100000005"))
                            {
                                uHD["statuscode"] = new OptionSetValue(100000003);
                                uHD["new_ngaykyhopdong"] = HD["new_ngayky"];
                            }
                            service.Update(uHD);

                            EntityCollection dsCTHDDT = FindCTHDDTmia(service, uHD);
                            if (dsCTHDDT != null && dsCTHDDT.Entities.Count > 0)
                            {
                                foreach (Entity a in dsCTHDDT.Entities)
                                {
                                    if (a.Contains("statuscode") && ((OptionSetValue)a["statuscode"]).Value.ToString() == "1")
                                    {
                                        if (a.Contains("new_dongiahopdong") || a.Contains("new_dongiahopdongkhl") || a.Contains("new_dongiaphanbonhd"))
                                        {
                                            Entity en = new Entity(a.LogicalName);
                                            en.Id = a.Id;

                                            en["statuscode"] = new OptionSetValue(100000000);
                                            en["new_trangthainghiemthu"] = new OptionSetValue(100000001);
                                            service.Update(en);
                                        }
                                    }
                                }
                            }
                        }
                        if (HD.Contains("new_hopdongdaututhuedat"))
                        {
                            Entity uHD = new Entity("new_hopdongthuedat");
                            uHD.Id = ((EntityReference)HD["new_hopdongdaututhuedat"]).Id;

                            uHD = service.Retrieve("new_hopdongthuedat", ((EntityReference)HD["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "statuscode" }));

                            if (uHD.Contains("statuscode") && (((OptionSetValue)uHD["statuscode"]).Value.ToString() == "1" || ((OptionSetValue)uHD["statuscode"]).Value.ToString() == "100000004"))
                            {
                                uHD["statuscode"] = new OptionSetValue(100000000);
                                uHD["new_ngaykyhopdong"] = HD["new_ngayky"];
                            }
                            service.Update(uHD);

                            EntityCollection dsCTHDDT = FindCTHDDTthuedat(service, uHD);
                            if (dsCTHDDT != null && dsCTHDDT.Entities.Count > 0)
                            {
                                foreach (Entity a in dsCTHDDT.Entities)
                                {
                                    if (a.Contains("new_trangthainghiemthu") && ((OptionSetValue)a["new_trangthainghiemthu"]).Value.ToString() == "100000000")
                                    {
                                        if (a.Contains("new_sotiendautu"))
                                        {
                                            Entity en = new Entity(a.LogicalName);
                                            en.Id = a.Id;

                                            en["new_trangthainghiemthu"] = new OptionSetValue(100000001);
                                            service.Update(en);
                                        }
                                    }
                                }
                            }
                        }

                        if (HD.Contains("new_hopdongthechap"))
                        {
                            Entity uHD = new Entity("new_hopdongthechap");
                            uHD.Id = ((EntityReference)HD["new_hopdongthechap"]).Id;
                            uHD = service.Retrieve("new_hopdongthechap", ((EntityReference)HD["new_hopdongthechap"]).Id, new ColumnSet(new string[] { "statuscode" }));

                            if (uHD.Contains("statuscode") && ((OptionSetValue)uHD["statuscode"]).Value.ToString() == "1")
                            {
                                uHD["statuscode"] = new OptionSetValue(100000000);
                                uHD["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                            }
                            service.Update(uHD);

                            EntityCollection dsTSTC = FindTSTC(service, uHD);
                            if (dsTSTC != null && dsTSTC.Entities.Count > 0)
                            {
                                foreach (Entity a in dsTSTC.Entities)
                                {
                                    if (a.Contains("new_giatrisosachgiatriquydinh") || a.Contains("new_giatridinhgiagiatrithechap"))
                                    {
                                        Entity en = new Entity(a.LogicalName);
                                        en.Id = a.Id;

                                        en["statuscode"] = new OptionSetValue(100000000);
                                        service.Update(en);
                                    }
                                }
                            }
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

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                        if (!upHD.Contains("new_ngaykyhopdong"))
                        {
                            upHD["new_ngaykyhopdong"] = DateTime.Now;
                            service.Update(upHD);
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

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                        if (!upHD.Contains("new_ngaykyhopdong"))
                        {
                            upHD["new_ngaykyhopdong"] = DateTime.Now;
                            service.Update(upHD);
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

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                        if (!upHD.Contains("new_ngaykyhopdong"))
                        {
                            upHD["new_ngaykyhopdong"] = DateTime.Now;
                            service.Update(upHD);
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

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                        if (!upHD.Contains("new_ngaykyhopdong"))
                        {
                            upHD["new_ngaykyhopdong"] = DateTime.Now;
                            service.Update(upHD);
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

                        List<Entity> lstChitiethdmuabanmiangoai = RetrieveMultiRecord(service, "new_chitiethopdongmuabanmiangoai",
                            new ColumnSet(new string[] { "statuscode" }), "new_hopdongmuabanmiangoai", target.Id);

                        foreach (Entity en in lstChitiethdmuabanmiangoai)
                        {
                            en["statuscode"] = new OptionSetValue(100000000);
                            service.Update(en);
                        }

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngayduyet" }));

                        if (!upHD.Contains("new_ngayduyet"))
                        {
                            upHD["new_ngayduyet"] = DateTime.Now;
                            service.Update(upHD);
                        }
                    }
            }
            else if (target.LogicalName == "new_hopdongdautuhatang")
            {
                if (target.Contains("new_tinhtrangduyet"))
                {
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity("new_hopdongdautuhatang");
                        traceService.Trace("1");
                        uHD.Id = HD.Id;
                        traceService.Trace("2");
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                        traceService.Trace("3");
                        if (HD.Contains("new_donvithicongkhachhang"))
                        {
                            traceService.Trace("4");
                            Entity KH = new Entity("contact");
                            KH.Id = ((EntityReference)HD["new_donvithicongkhachhang"]).Id;
                            KH["statuscode"] = new OptionSetValue(100000000);
                            service.Update(KH);
                        }
                        else if (HD.Contains("new_donvithicongkhdn"))
                        {
                            traceService.Trace("5");
                            Entity KH = new Entity("account");
                            traceService.Trace("5");
                            KH.Id = ((EntityReference)HD["new_donvithicongkhdn"]).Id;
                            traceService.Trace("5");
                            KH["statuscode"] = new OptionSetValue(100000000);
                            traceService.Trace("5");
                            service.Update(KH);
                        }

                        Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                        if (!upHD.Contains("new_ngaykyhopdong"))
                        {
                            upHD["new_ngaykyhopdong"] = DateTime.Now;
                            service.Update(upHD);
                        }
                    }
                }
            }
            #endregion
            else if (target.LogicalName == "new_phieudangkydichvu"
              || target.LogicalName == "new_phieudangkyhomgiong"
              || target.LogicalName == "new_phieudangkyphanbon"
              || target.LogicalName == "new_phieudangkythuoc"
              || target.LogicalName == "new_phieudangkyvattu"
              || target.LogicalName == "new_bienbanvipham"
              )
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

            // còn thiếu cập nhật trạng thái các phiếu kèm phiếu chi.
            else if (target.LogicalName == "new_phieuchitienmat")
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
            else if (target.LogicalName == "new_bienbangiamhuydientich"
                 || target.LogicalName == "new_bienbanmiachay"
                )
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000004)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000001);
                        service.Update(uHD);
                    }
            }

            else if (target.LogicalName == "new_denghiruthoso"
                 || target.LogicalName == "new_nghiemthucongtrinh"
                 || target.LogicalName == "new_phieudenghigiaingan"
                )
            {
                if (target.Contains("new_tinhtrangduyet"))
                {
                    traceService.Trace("Begin new_denghiruthoso");
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000007)
                    {
                        traceService.Trace("new_denghiruthoso qqq");
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
                }
            }

            else if (target.LogicalName == "new_nghiemthuthuedat"
                   || target.LogicalName == "new_nghiemthutrongmia"
                   || target.LogicalName == "new_nghiemthutuoimia"
                   || target.LogicalName == "new_nghiemthumaymocthietbi"
                   || target.LogicalName == "new_phieudenghithuno"
                   || target.LogicalName == "new_phieutamung"
                   || target.LogicalName == "new_nghiemthuboclamia"
                   //|| target.LogicalName == "new_bangketienmia"
                   || target.LogicalName == "new_nghiemthukhac"
                   || target.LogicalName == "new_nghiemthuchatsatgoc"
                   || target.LogicalName == "new_nghiemthudichvu"
                   || target.LogicalName == "new_phieudenghithanhtoan")
            {
                if (target.Contains("new_tinhtrangduyet"))
                {
                    int tinhtrangduyet = ((OptionSetValue)target["new_tinhtrangduyet"]).Value;
                    if (tinhtrangduyet == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }

                    if (tinhtrangduyet == 100000006 && target.LogicalName == "new_nghiemthumaymocthietbi")
                    {
                        List<Entity> lstChitietNTMMTB = RetrieveMultiRecord(service, "new_chitietnghiemthummtb",
                            new ColumnSet(new string[] { "new_maymocthietbi" }), "new_nghiemthummtb", target.Id);

                        Entity ntmmtb = service.Retrieve(target.LogicalName, target.Id,
                            new ColumnSet(new string[] { "new_hopdongdaututrangthietbi" }));

                        if (!ntmmtb.Contains("new_hopdongdaututrangthietbi"))
                            throw new Exception("Nghiệm thu máy móc thiết bị không có hợp đồng trang thiết bị");

                        EntityReference hdtrangthietbi = (EntityReference)ntmmtb["new_hopdongdaututrangthietbi"];

                        foreach (Entity en in lstChitietNTMMTB)
                        {
                            if (!en.Contains("new_maymocthietbi"))
                                throw new Exception("Chi tiết nghiệm thu máy móc thiết bị không có máy móc thiết bị");

                            EntityReference mmtb = (EntityReference)en["new_maymocthietbi"];

                            QueryExpression q = new QueryExpression("new_hopdongdaututrangthietbichitiet");
                            q.ColumnSet = new ColumnSet(new string[] { "new_trangthainghiemthu", "new_name" });
                            q.Criteria = new FilterExpression();
                            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdaututrangthietbi", ConditionOperator.Equal, hdtrangthietbi.Id));
                            q.Criteria.AddCondition(new ConditionExpression("new_maymocthietbi", ConditionOperator.Equal, mmtb.Id));
                            EntityCollection entc = service.RetrieveMultiple(q);

                            if (entc.Entities.Count > 0)
                            {
                                entc.Entities[0]["new_trangthainghiemthu"] = new OptionSetValue(100000002);
                                service.Update(entc.Entities[0]);
                            }
                        }
                    }
                }
            }

            else if (target.LogicalName == "opportunity")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000004)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000005);
                        service.Update(uHD);
                        //throw new Exception("cap nhat xong trang thai " + uHD["statuscode"].ToString());
                    }
            }
            else if (target.LogicalName == "new_phieugiaonhanphanbon"
                || target.LogicalName == "new_phieugiaonhanthuoc"
                || target.LogicalName == "new_phieugiaonhanvattu"
                || target.LogicalName == "new_phuluchopdong"
                || target.LogicalName == "new_phieudenghithuong"
                || target.LogicalName == "new_phieuchuyenno"
                || target.LogicalName == "new_bangketienkhuyenkhich"
                )
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000005)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName == "new_phieugiaonhanhomgiong")
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
            else if (target.LogicalName == "new_phieudieuchinhcongno"
                    || target.LogicalName == "new_bangkechitiencuoivu")
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000001);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName == "new_bienbanthoathuancongdon"
                    || target.LogicalName == "new_bienbanthuhoachsom"
                )
            {
                if (target.Contains("new_tinhtrangduyet"))

                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000003)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName == "new_bienbancando"
                 || target.LogicalName == "new_danhgianangsuat"
                )
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000002)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000001);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName == "lead"
                )
            {
                if (target.Contains("new_tinhtrangduyet"))
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000001)
                    {
                        Entity uHD = new Entity(target.LogicalName);
                        uHD.Id = HD.Id;
                        uHD["statuscode"] = new OptionSetValue(100000000);
                        service.Update(uHD);
                    }
            }
            else if (target.LogicalName == "new_bangketienmia")
            {
                // phiếu tính tiền mía
                List<Entity> listphieutinhtienmia = RetrieveMultiRecord(service, "new_phieutinhtienmia",
                    new ColumnSet(new String[] { "statuscode", "new_pdnthuno", "new_pdnthuno_thuhoach",
                                "new_pdnthuno_vanchuyen","new_name","new_khachhang","new_khachhangdoanhnghiep" }), "new_bangke", target.Id);
                traceService.Trace(listphieutinhtienmia.Count.ToString());
                if (target.Contains("new_tinhtrangduyet"))
                {
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006) // ke toan truong da duyet chuyen khoan
                    {
                        Entity t = service.Retrieve(target.LogicalName, target.Id,
                            new ColumnSet(new string[] { "new_ngayduyetchuyenkhoan", "statuscode" }));

                        t["statuscode"] = new OptionSetValue(100000000);
                        t["new_ngayduyetchuyenkhoan"] = DateTime.Now;
                        service.Update(t);

                        // lệnh chi
                        EntityCollection listlenhdon = RetrieveNNRecord("new_lenhdon", target.LogicalName,
                            "new_new_bangketienmia_new_lenhdon", new ColumnSet(new string[] { "statuscode" }),
                            "new_bangketienmiaid", target.Id);

                        if (listlenhdon != null && listlenhdon.Entities.Count > 0)
                        {
                            foreach (var item in listlenhdon.Entities)
                            {
                                Entity lenhdon = new Entity("new_lenhdon");
                                lenhdon.Id = item.Id;
                                lenhdon["statuscode"] = new OptionSetValue(100000003);
                                service.Update(lenhdon);
                            }
                        }

                        if (listphieutinhtienmia != null && listphieutinhtienmia.Count > 0)
                        {
                            foreach (var lst in listphieutinhtienmia)
                            {
                                traceService.Trace(lst["new_name"].ToString());

                                Entity pttm = service.Retrieve(lst.LogicalName, lst.Id,
                                    new ColumnSet(new string[] {"statuscode"}));
                                pttm["statuscode"] = new OptionSetValue(100000000);
                                traceService.Trace("Phieu tinh tien mia");
                                service.Update(pttm);

                                if (lst.Contains("new_pdnthuno"))
                                {
                                    Entity chumia = service.Retrieve("new_phieudenghithuno", ((EntityReference)lst["new_pdnthuno"]).Id,
                                        new ColumnSet(new string[] { "statuscode", "new_ngayduyet", "new_tinhtrangduyet" }));

                                    chumia["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                    chumia["new_ngayduyet"] = DateTime.Now;
                                    chumia["statuscode"] = new OptionSetValue(100000000);
                                    traceService.Trace("chu mia");
                                    service.Update(chumia);
                                }

                                if (lst.Contains("new_pdnthuno_thuhoach"))
                                {
                                    Entity thuhoach = service.Retrieve("new_phieudenghithuno", ((EntityReference)lst["new_pdnthuno_thuhoach"]).Id,
                                        new ColumnSet(new string[] { "statuscode", "new_ngayduyet", "new_tinhtrangduyet" }));

                                    thuhoach["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                    thuhoach["new_ngayduyet"] = DateTime.Now;
                                    thuhoach["statuscode"] = new OptionSetValue(100000000);
                                    traceService.Trace("thu hoach");
                                    service.Update(thuhoach);
                                }

                                if (lst.Contains("new_pdnthuno_vanchuyen"))
                                {
                                    Entity vanchuyen = service.Retrieve("new_phieudenghithuno", ((EntityReference)lst["new_pdnthuno_vanchuyen"]).Id,
                                        new ColumnSet(new string[] { "statuscode", "new_ngayduyet", "new_tinhtrangduyet" }));

                                    vanchuyen["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                    vanchuyen["new_ngayduyet"] = DateTime.Now;
                                    vanchuyen["statuscode"] = new OptionSetValue(100000000);
                                    traceService.Trace("van chuyen");
                                    service.Update(vanchuyen);
                                }
                            }
                        }
                    }
                    else if (((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000004) // ke toan truong da duyet tien mat
                    {
                        Entity t = service.Retrieve(target.LogicalName, target.Id,
                           new ColumnSet(new string[] { "new_ngayduyet" }));

                        t["new_ngayduyet"] = DateTime.Now;
                        service.Update(t);

                        // lệnh chi
                        EntityCollection listlenhdon = RetrieveNNRecord("new_lenhdon", target.LogicalName,
                            "new_new_bangketienmia_new_lenhdon", new ColumnSet(new string[] { "statuscode" }),
                            "new_bangketienmiaid", target.Id);

                        if (listlenhdon != null && listlenhdon.Entities.Count > 0)
                        {
                            foreach (var item in listlenhdon.Entities)
                            {
                                Entity lenhdon = new Entity("new_lenhdon");
                                lenhdon.Id = item.Id;
                                lenhdon["statuscode"] = new OptionSetValue(100000003);
                                service.Update(lenhdon);
                            }
                        }

                        for (int i = 0; i < listphieutinhtienmia.Count; i++)
                        {
                            Entity lst = listphieutinhtienmia[i];

                            if (CheckPTTMTienMat(lst) == false)
                            {
                                continue;
                            }
                            traceService.Trace(lst["new_name"].ToString());

                            Entity pttm = service.Retrieve(lst.LogicalName, lst.Id,
                                    new ColumnSet(new string[] { "statuscode" }));
                            pttm["statuscode"] = new OptionSetValue(100000000);
                            traceService.Trace("Phieu tinh tien mia");
                            service.Update(pttm);

                            if (lst.Contains("new_pdnthuno"))
                            {
                                Entity chumia = service.Retrieve("new_phieudenghithuno", ((EntityReference)lst["new_pdnthuno"]).Id,
                                    new ColumnSet(new string[] { "statuscode", "new_ngayduyet", "new_tinhtrangduyet" }));

                                chumia["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                chumia["new_ngayduyet"] = DateTime.Now;
                                chumia["statuscode"] = new OptionSetValue(100000000);
                                traceService.Trace("chu mia");
                                service.Update(chumia);
                            }
                            if (lst.Contains("new_pdnthuno_thuhoach"))
                            {
                                Entity thuhoach = service.Retrieve("new_phieudenghithuno", ((EntityReference)lst["new_pdnthuno_thuhoach"]).Id,
                                    new ColumnSet(new string[] { "statuscode", "new_ngayduyet", "new_tinhtrangduyet" }));

                                thuhoach["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                thuhoach["new_ngayduyet"] = DateTime.Now;
                                thuhoach["statuscode"] = new OptionSetValue(100000000);
                                traceService.Trace("thu hoach");
                                service.Update(thuhoach);
                            }
                            if (lst.Contains("new_pdnthuno_vanchuyen"))
                            {
                                Entity vanchuyen = service.Retrieve("new_phieudenghithuno", ((EntityReference)lst["new_pdnthuno_vanchuyen"]).Id,
                                    new ColumnSet(new string[] { "statuscode", "new_ngayduyet", "new_tinhtrangduyet" }));

                                vanchuyen["new_tinhtrangduyet"] = new OptionSetValue(100000006);
                                vanchuyen["new_ngayduyet"] = DateTime.Now;
                                vanchuyen["statuscode"] = new OptionSetValue(100000000);
                                traceService.Trace("van chuyen");
                                service.Update(vanchuyen);
                            }

                            listphieutinhtienmia.RemoveAt(i);
                            i--;
                        }
                    }
                }
            }
            else if (target.LogicalName == "new_hopdongdautumia")
            {
                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000003) // ky hd
                {
                    Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                    if (!upHD.Contains("new_ngaykyhopdong"))
                    {
                        upHD["new_ngaykyhopdong"] = DateTime.Now;
                        service.Update(upHD);
                    }
                }
            }
            else if (target.LogicalName == "new_hopdongthuedat")
            {
                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // ky hd
                {
                    Entity upHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_ngaykyhopdong" }));

                    if (!upHD.Contains("new_ngaykyhopdong"))
                    {
                        upHD["new_ngaykyhopdong"] = DateTime.Now;
                        service.Update(upHD);
                    }
                }
            }
        }

        public static EntityCollection FindCTHDDTmia(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_thuadatcanhtac'>
                        <attribute name='new_name' />
                        <attribute name='statuscode' />
                        <attribute name='new_dongiahopdong' />
                        <attribute name='new_dongiahopdongkhl' />
                        <attribute name='new_dongiaphanbonhd' />
                        <attribute name='new_thuadatcanhtacid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCTHDDTthuedat(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_datthue'>
                        <attribute name='new_name' />
                        <attribute name='new_sotiendautu' />
                        <attribute name='new_trangthainghiemthu' />
                        <attribute name='new_datthueid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongthuedat' operator='eq' uitype='new_hopdongthuedat' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindTSTC(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_taisanthechap'>
                        <attribute name='new_taisanthechapid' />
                        <attribute name='new_name' />
                        <attribute name='statuscode' />
                        <attribute name='new_giatrisosachgiatriquydinh' />
                        <attribute name='new_giatridinhgiagiatrithechap' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongthechap' operator='eq' uitype='new_hopdongthechap' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        EntityCollection RetrieveNNRecord(string entity1, string entity2, string relateName, ColumnSet column, string entity2condition, object entity2value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(entity2condition, ConditionOperator.Equal, entity2value));
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
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

        bool CheckPTTMTienMat(Entity pttmEntity)
        {
            Entity KH = null;

            if (pttmEntity.Contains("new_khachhang"))
            {
                KH = service.Retrieve("contact", ((EntityReference)pttmEntity["new_khachhang"]).Id,
                    new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
            }
            else if (pttmEntity.Contains("new_khachhangdoanhnghiep"))
            {
                KH = service.Retrieve("account", ((EntityReference) pttmEntity["new_khachhangdoanhnghiep"]).Id,
                    new ColumnSet(new string[] {"new_phuongthucthanhtoan"}));
            }
            else
                return false;

            if (((OptionSetValue)KH["new_phuongthucthanhtoan"]).Value == 100000000)
            {
                return true;
            }

            return false;
        }
    }
}
