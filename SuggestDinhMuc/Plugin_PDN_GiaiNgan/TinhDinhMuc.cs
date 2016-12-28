using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Text;

namespace Plugin_PDN_GiaiNgan
{
    public class TinhDinhMuc
    {
        private IOrganizationService service = null;
        private ITracingService traceService = null;
        private EntityReference pdkRef = null;
        private Dictionary<Guid, DataCollection<Entity>> css = new Dictionary<Guid, DataCollection<Entity>>();
        public TinhDinhMuc(IOrganizationService service, ITracingService traceService, EntityReference pdkRef)
        {
            this.service = service;
            this.traceService = traceService;
            this.pdkRef = pdkRef;
        }

        public void CalculateTrongMia(Entity pdk)
        {
            Entity PDNGN = service.Retrieve("new_phieudenghigiaingan", ((EntityReference)pdk["new_phieudenghigiaingan"]).Id,
                new ColumnSet(true));

            if (!PDNGN.Contains("new_hopdongdautumia") && !PDNGN.Contains("new_hopdongdaututhuedat") && !PDNGN.Contains("new_hopdongdautummtb"))
                throw new Exception("Vui lòng chọn loại hợp đồng trên phiếu đề nghị giải ngân !");

            traceService.Trace(((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString());
            if (pdk == null)
                throw new Exception(string.Format("Chi tiết đề nghị giải ngân không tồn tại hoặc bị xóa!", pdkRef.Name));

            //string pdk_name = pdk.Contains("new_name") ? (" '" + pdk["new_name"].ToString() + "'") : "";

            QueryExpression query = new QueryExpression("new_thuadat_pdngiaingan");
            query.ColumnSet = new ColumnSet(new string[] { "new_chitiethopdong", "new_chitietphieudenghigiaingan" });
            query.Criteria = new FilterExpression(LogicalOperator.And);
            query.Criteria.AddCondition("new_chitietphieudenghigiaingan", ConditionOperator.Equal, pdk.Id);

            EntityCollection etnc = service.RetrieveMultiple(query);

            foreach (Entity a in etnc.Entities)
                service.Delete(a.LogicalName, a.Id);

            decimal dmhl = 0;
            decimal dmhlvt = 0;
            decimal dm0hl = 0;
            //----------------------------
            decimal gnhltm = 0;
            decimal gnhlvt = 0;
            decimal gn0hl = 0;

            if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value == 100000000) //HD mía
            {
                if (!PDNGN.Contains("new_hopdongdautumia"))
                    throw new Exception(string.Format("Vui lòng chọn hợp đầu tư mía cho chi tiết đề nghị giải ngân '{0}'!", pdkRef.Name));

                EntityReference hdRef = (EntityReference)PDNGN["new_hopdongdautumia"];
                Entity hd = service.Retrieve(hdRef.LogicalName, hdRef.Id,
                    new ColumnSet(new string[] { "new_dinhmucphanbontoithieu", "new_dinhmucdautukhonghoanlai", "new_dinhmucdautucohoanlai" }));

                if (hd == null)
                    throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!"));

                if (!pdk.Contains("new_noidunggiaingan"))
                    throw new Exception("Bạn chưa điền nội dung giải ngân cho chi tiết giải ngân này !");

                EntityCollection cthds = new EntityCollection();

                if (((OptionSetValue)pdk["new_noidunggiaingan"]).Value == 100000000) //Dau tu mia
                {
                    #region Dau tu mia
                    query = new QueryExpression("new_thuadatcanhtac");
                    query.ColumnSet = new ColumnSet(new string[] {
                        "new_name",
                        "new_trangthainghiemthu",
                        "new_chinhsachdautu",
                        "statuscode",
                        "new_yeucaudacbiet",
                        "new_dinhmucdautuhoanlai_hientai",
                        "new_dinhmucdautukhonghoanlai_hientai",
                        "new_dinhmucphanbontt",
                        "new_conlai_hoanlai",
                        "new_conlai_phanbontoithieu",
                        "new_conlai_khonghoanlai",
                        //----------------------------
                        "new_dachikhonghoanlai_tienmat",
                        //----------------------------
                        "new_dachihoanlai_tienmat",
                        "new_dachihoanlai_homgiong",
                        "new_dachihoanlai_phanbon",
                        "new_dachihoanlai_thuoc",
                        "new_dachihoanlai_vattukhac",
                        "new_dachihoanlai_dichvu",
                    });
                    query.Criteria = new FilterExpression(LogicalOperator.And);
                    query.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hd.Id));
                    query.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));//đã ký
                    query.Criteria.AddCondition(new ConditionExpression("new_trangthainghiemthu", ConditionOperator.GreaterThan, 100000000));//khác nháp
                    cthds = service.RetrieveMultiple(query);
                    if (cthds.Entities.Count == 0)
                        return;

                    foreach (Entity cthd in cthds.Entities)
                    {
                        decimal tyle = GetTyle(((EntityReference)cthd["new_chinhsachdautu"]).Id, ((OptionSetValue)cthd["new_trangthainghiemthu"]).Value, (cthd.Contains("new_yeucaudacbiet") ? (bool)cthd["new_yeucaudacbiet"] : false));

                        dmhl += tyle / 100 * (cthd.Contains("new_conlai_hoanlai") ? ((Money)cthd["new_conlai_hoanlai"]).Value : 0);
                        dmhlvt += tyle / 100 * (cthd.Contains("new_conlai_phanbontoithieu") ? ((Money)cthd["new_conlai_phanbontoithieu"]).Value : 0);
                        dm0hl += tyle / 100 * (cthd.Contains("new_conlai_khonghoanlai") ? ((Money)cthd["new_conlai_khonghoanlai"]).Value : 0);

                        #region Them chi tiet hop va phieu dang ky

                        string ct_name = cthd.Contains("new_name") ? (cthd["new_name"].ToString() + "-") : "";

                        Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                        ct_pdk["new_chitiethopdong"] = new EntityReference(cthd.LogicalName, cthd.Id);
                        ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                        ct_pdk["new_name"] = string.Format("{0}-{1}", ct_name, pdk["new_name"]);
                        ct_pdk["new_dmhltm"] = new Money(dmhl);
                        ct_pdk["new_dmhlvt"] = new Money(dmhlvt);
                        ct_pdk["new_dm0hl"] = new Money(dm0hl);
                        service.Create(ct_pdk);

                        #endregion

                        gnhltm += cthd.Contains("new_dachihoanlai_tienmat") ? ((Money)cthd["new_dachihoanlai_tienmat"]).Value : 0;
                        gnhltm += cthd.Contains("new_dachihoanlai_dichvu") ? ((Money)cthd["new_dachihoanlai_dichvu"]).Value : 0;

                        gnhlvt = cthd.Contains("new_dachihoanlai_homgiong") ? ((Money)cthd["new_dachihoanlai_homgiong"]).Value : 0;
                        gnhlvt += cthd.Contains("new_dachihoanlai_phanbon") ? ((Money)cthd["new_dachihoanlai_phanbon"]).Value : 0;
                        gnhlvt += cthd.Contains("new_dachihoanlai_thuoc") ? ((Money)cthd["new_dachihoanlai_thuoc"]).Value : 0;
                        gnhlvt += cthd.Contains("new_dachihoanlai_vattukhac") ? ((Money)cthd["new_dachihoanlai_vattukhac"]).Value : 0;
                        gn0hl += cthd.Contains("new_dachikhonghoanlai_tienmat") ? ((Money)cthd["new_dachikhonghoanlai_tienmat"]).Value : 0;
                    }

                    Sum_pdn(ref gnhltm, ref gn0hl, hdRef, ((EntityReference)pdk["new_phieudenghigiaingan"]).Id);
                    sum_pdk(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl);

                    #endregion
                }
                else
                {
                    if (((OptionSetValue)pdk["new_noidunggiaingan"]).Value == 100000001) //Tuoi
                    {
                        #region NT Tuoi
                        if (!pdk.Contains("new_nghiemthutuoi"))
                            throw new Exception("Chưa nhập thông tin phiếu Nghiệm thu tưới");
                        Entity NT = service.Retrieve("new_nghiemthutuoimia", ((EntityReference)pdk["new_nghiemthutuoi"]).Id, new ColumnSet(true));

                        query = new QueryExpression("new_chitietnghiemthutuoimia");
                        query.ColumnSet = new ColumnSet(new string[] { "new_thuadat", "new_dautuhl", "new_dautukhl" });
                        query.Criteria.AddCondition(new ConditionExpression("new_nghiemthutuoimia", ConditionOperator.Equal, NT.Id));

                        EntityCollection rs = service.RetrieveMultiple(query);
                        Dictionary<Guid, Entity> tds = new Dictionary<Guid, Entity>();
                        List<Guid> td = new List<Guid>();
                        foreach (Entity a in rs.Entities)
                        {
                            td.Add(a.Id);
                            tds.Add(((EntityReference)a["new_thuadat"]).Id, a);

                        }

                        query = new QueryExpression("new_thuadatcanhtac");
                        query.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_thuadat" });
                        query.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, ((EntityReference)NT["new_hopdongtrongmia"]).Id));
                        query.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.In, td.ToArray()));

                        EntityCollection rs2 = service.RetrieveMultiple(query);

                        foreach (Entity a in rs2.Entities)
                        {
                            Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                            ct_pdk["new_chitiethopdong"] = new EntityReference("new_thuadatcanhtac", a.Id);
                            ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                            ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                            ct_pdk["new_dmhltm"] = tds[((EntityReference)a["new_thuadat"]).Id]["new_dautuhl"];
                            ct_pdk["new_dmhlvt"] = new Money(0);
                            ct_pdk["new_dm0hl"] = tds[((EntityReference)a["new_thuadat"]).Id]["new_dautukhl"];
                            service.Create(ct_pdk);
                        }

                        dmhl = NT.Contains("new_dautuhl") ? ((Money)NT["new_dautuhl"]).Value : (decimal)0;
                        dm0hl = NT.Contains("new_dautukhl") ? ((Money)NT["new_dautukhl"]).Value : (decimal)0;
                        Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdautumia", hdRef, 100000000, "new_nghiemthutuoi", NT.Id);
                        sum_pdkNT(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl, 100000000, "new_nghiemthutuoimia", NT.Id);

                        #endregion
                    }
                    else if (((OptionSetValue)pdk["new_noidunggiaingan"]).Value == 100000002) // Bóc lá mía
                    {
                        #region NT Bocla
                        if (!pdk.Contains("new_nghiemthuboclamia"))
                            throw new Exception("Chưa nhập thông tin phiếu Nghiệm thu bóc lá mía");
                        Entity NT = service.Retrieve("new_nghiemthuboclamia", ((EntityReference)pdk["new_nghiemthuboclamia"]).Id, new ColumnSet(true));

                        query = new QueryExpression("new_nghiemthuboclamiathuadat");
                        query.ColumnSet = new ColumnSet(new string[] { "new_chitiethddtmia", "new_sotien" });
                        query.Criteria.AddCondition(new ConditionExpression("new_nghiemthuboclamia", ConditionOperator.Equal, NT.Id));

                        EntityCollection rs = service.RetrieveMultiple(query);

                        foreach (Entity a in rs.Entities)
                        {
                            Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                            ct_pdk["new_chitiethopdong"] = new EntityReference("new_thuadatcanhtac", ((EntityReference)a["new_chitiethddtmia"]).Id);
                            ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                            ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                            ct_pdk["new_dmhltm"] = new Money(0);
                            ct_pdk["new_dmhlvt"] = new Money(0);
                            ct_pdk["new_dm0hl"] = a["new_sotien"];
                            service.Create(ct_pdk);
                        }

                        //dmhl = NT.Contains("new_dautuhl") ? ((Money)NT["new_dautuhl"]).Value : (decimal)0;
                        dm0hl = NT.Contains("new_tongtien") ? ((Money)NT["new_tongtien"]).Value : (decimal)0;
                        Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdautumia", hdRef, 100000001, "new_nghiemthuboclamia", NT.Id);
                        sum_pdkNT(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl, 100000001, "new_nghiemthuboclamia", NT.Id);

                        #endregion
                    }
                    else if (((OptionSetValue)pdk["new_noidunggiaingan"]).Value == 100000003) //NT đầu tư bổ sung vốn
                    {
                        #region NT Dautubosungvon

                        if (!pdk.Contains("new_danhgianangsuat"))
                            throw new Exception("Chưa nhập thông tin phiếu Nghiệm thu đầu tư bổ sung vốn");
                        Entity NT = service.Retrieve("new_danhgianangsuat", ((EntityReference)pdk["new_danhgianangsuat"]).Id, new ColumnSet(true));

                        Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                        ct_pdk["new_chitiethopdong"] = new EntityReference("new_thuadatcanhtac", ((EntityReference)NT["new_thuadatcanhtac"]).Id);
                        ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                        ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                        ct_pdk["new_dmhltm"] = NT["new_denghihoanlaitienmat"];
                        ct_pdk["new_dmhlvt"] = NT["new_denghihoanlaivattu"];
                        ct_pdk["new_dm0hl"] = NT["new_denghikhl"];
                        service.Create(ct_pdk);

                        dmhl = (NT.Contains("new_denghihoanlaitienmat") ? ((Money)NT["new_denghihoanlaitienmat"]).Value : (decimal)0) + (NT.Contains("new_denghihoanlaivattu") ? ((Money)NT["new_denghihoanlaivattu"]).Value : (decimal)0);
                        dm0hl = NT.Contains("new_denghikhl") ? ((Money)NT["new_denghikhl"]).Value : (decimal)0;
                        Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdautumia", hdRef, 100000002, "new_danhgianangsuat", NT.Id);
                        sum_pdkNT(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl, 100000002, "new_nghiemthudautubosungvon", NT.Id);

                        #endregion
                    }
                    else if (((OptionSetValue)pdk["new_noidunggiaingan"]).Value == 100000004) //NT khác
                    {
                        #region NT Khac
                        if (!pdk.Contains("new_nghiemthukhac"))
                            throw new Exception("Chưa nhập thông tin phiếu Nghiệm thu khác");
                        Entity NT = service.Retrieve("new_nghiemthukhac", ((EntityReference)pdk["new_nghiemthukhac"]).Id, new ColumnSet(true));

                        Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                        ct_pdk["new_chitiethopdong"] = new EntityReference("new_thuadatcanhtac", ((EntityReference)NT["new_chitiethddtmia"]).Id);
                        ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                        ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                        ct_pdk["new_dmhltm"] = NT.Contains("new_dautuhl") ? NT["new_dautuhl"] : new Money(0);
                        ct_pdk["new_dmhlvt"] = new Money(0);
                        ct_pdk["new_dm0hl"] = NT.Contains("new_dautukhl") ? NT["new_dautukhl"] : new Money(0);
                        service.Create(ct_pdk);

                        dmhl = NT.Contains("new_dautuhl") ? ((Money)NT["new_dautuhl"]).Value : (decimal)0;
                        dm0hl = NT.Contains("new_dautukhl") ? ((Money)NT["new_dautukhl"]).Value : (decimal)0;

                        Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdautumia", hdRef, 100000003, "new_nghiemthukhac", NT.Id);
                        sum_pdkNT(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl, 100000003, "new_nghiemthukhac", NT.Id);
                        #endregion
                    }
                    else if (((OptionSetValue)pdk["new_noidunggiaingan"]).Value == 100000005) //Tạm ứng
                    {
                        #region Tạm Ứng    

                        if (!pdk.Contains("new_phieudenghitamung"))
                            throw new Exception("Chưa nhập thông tin phiếu đề nghị tạm ứng");
                        Entity NT = service.Retrieve("new_phieutamung", ((EntityReference)pdk["new_phieudenghitamung"]).Id, new ColumnSet(true));

                        query = new QueryExpression("new_chitietphieudenghitamung");
                        query.ColumnSet = new ColumnSet(new string[] { "new_chitiethddtmia", "new_dinhmuctamung" });
                        query.Criteria.AddCondition(new ConditionExpression("new_phieudenghitamung", ConditionOperator.Equal, NT.Id));

                        EntityCollection rs = service.RetrieveMultiple(query);

                        foreach (Entity a in rs.Entities)
                        {
                            Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                            ct_pdk["new_chitiethopdong"] = new EntityReference("new_thuadatcanhtac", ((EntityReference)a["new_chitiethddtmia"]).Id);
                            ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                            ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                            ct_pdk["new_dmhltm"] = a.Contains("new_dinhmuctamung") ? a["new_dinhmuctamung"] : new Money(0);
                            ct_pdk["new_dmhlvt"] = new Money(0);
                            ct_pdk["new_dm0hl"] = new Money(0);
                            service.Create(ct_pdk);
                        }

                        dmhl = NT.Contains("new_sotienung") ? ((Money)NT["new_sotienung"]).Value : (decimal)0;
                        //dm0hl = NT.Contains("new_tongtien") ? ((Money)NT["new_tongtien"]).Value : (decimal)0;
                        Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdautumia", hdRef, 100000004, "new_phieudenghitamung", NT.Id);
                        //sum_pdkNT(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl, 100000001, "new_phieudenghitamung", NT.Id);
                        #endregion
                    }
                }
            }
            else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value == 100000001) //HD thue dat
            {
                #region NT thuê đất
                traceService.Trace("hd thue dat");
                EntityReference hdRef = (EntityReference)PDNGN["new_hopdongdaututhuedat"];
                Entity hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(true));

                if (!pdk.Contains("new_nghiemthuthuedat"))
                    throw new Exception("Chưa nhập thông tin phiếu Nghiệm thu thuê đất");
                Entity NT = service.Retrieve("new_nghiemthuthuedat", ((EntityReference)pdk["new_nghiemthuthuedat"]).Id, new ColumnSet(true));

                query = new QueryExpression("new_chitietnghiemthuthuedat");
                query.ColumnSet = new ColumnSet(new string[] { "new_thuadat", "new_sotiendautu" });
                query.Criteria.AddCondition(new ConditionExpression("new_nghiemthuthuedat", ConditionOperator.Equal, NT.Id));

                EntityCollection rs = service.RetrieveMultiple(query);

                foreach (Entity a in rs.Entities)
                {
                    Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                    ct_pdk["new_thuadat"] = new EntityReference("new_thuadat", ((EntityReference)a["new_thuadat"]).Id);
                    ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                    ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                    ct_pdk["new_dmhltm"] = a.Contains("new_sotiendautu") ? a["new_sotiendautu"] : new Money(0);
                    ct_pdk["new_dmhlvt"] = new Money(0);
                    ct_pdk["new_dm0hl"] = new Money(0);
                    service.Create(ct_pdk);
                }

                dmhl = NT.Contains("new_tongtien") ? ((Money)NT["new_tongtien"]).Value : (decimal)0;
                traceService.Trace(dmhl.ToString());
                //dm0hl = NT.Contains("new_dautukhl") ? ((Money)NT["new_dautukhl"]).Value : (decimal)0;
                Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdaututhuedat", hdRef, -1, "new_nghiemthuthuedat", NT.Id);
                //sum_pdkNT(hdRef, pdkRef, ref gnhltm, ref gnhlvt, ref gn0hl, 100000000, "new_nghiemthutuoimia", NT.Id);
                #endregion
            }
            else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value == 100000002) // HD MMTB
            {
                traceService.Trace("start");
                EntityReference hdRef = (EntityReference)PDNGN["new_hopdongdautummtb"];
                Entity hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(true));
                
                #region NT MMTB

                if (!pdk.Contains("new_nghiemthummtb"))
                    throw new Exception("Chưa nhập thông tin phiếu Nghiệm thu MMTB !");
                                
                Entity NT = service.Retrieve("new_nghiemthumaymocthietbi", ((EntityReference)pdk["new_nghiemthummtb"]).Id,
                    new ColumnSet(true));
                
                query = new QueryExpression("new_chitietnghiemthummtb");
                query.ColumnSet = new ColumnSet(new string[] { "new_maymocthietbi", "new_giamua" });
                query.Criteria.AddCondition(new ConditionExpression("new_nghiemthummtb", ConditionOperator.Equal, NT.Id));

                EntityCollection rs = service.RetrieveMultiple(query);
                Dictionary<Guid, Entity> tds = new Dictionary<Guid, Entity>();
                List<Guid> td = new List<Guid>();
                
                foreach (Entity a in rs.Entities)
                {
                    traceService.Trace(CheckDuplicateGuid(td, a.Id).ToString());
                    
                    if (CheckDuplicateGuid(td, a.Id) == true)
                    {
                        td.Add(a.Id);
                        tds.Add(((EntityReference)a["new_maymocthietbi"]).Id, a);
                    }
                }
                traceService.Trace("so luong mmtb : " + td.Count.ToString());
                query = new QueryExpression("new_hopdongdaututrangthietbichitiet");
                query.ColumnSet = new ColumnSet(new string[] { "new_hopdongdaututrangthietbichitietid", "new_maymocthietbi" });
                query.Criteria.AddCondition(new ConditionExpression("new_hopdongdaututrangthietbi", ConditionOperator.Equal, ((EntityReference)NT["new_hopdongdaututrangthietbi"]).Id));
                query.Criteria.AddCondition(new ConditionExpression("new_maymocthietbi", ConditionOperator.In, td.ToArray()));

                EntityCollection rs2 = service.RetrieveMultiple(query);
                traceService.Trace("a");
                foreach (Entity a in rs2.Entities)
                {
                    Entity ct_pdk = new Entity("new_thuadat_pdngiaingan");
                    ct_pdk["new_hopdongdaututrangthietbichitiet"] = new EntityReference("new_hopdongdaututrangthietbichitiet", a.Id);
                    ct_pdk["new_chitietphieudenghigiaingan"] = pdkRef;
                    ct_pdk["new_name"] = string.Format("{0}", pdk["new_name"]);
                    ct_pdk["new_dmhltm"] = tds[((EntityReference)a["new_maymocthietbi"]).Id]["new_giamua"];
                    ct_pdk["new_dmhlvt"] = new Money(0);
                    ct_pdk["new_dm0hl"] = new Money(0);
                    service.Create(ct_pdk);
                }

                dmhl = NT.Contains("new_tonggiatridautuhl") ? ((Money)NT["new_tonggiatridautuhl"]).Value : (decimal)0;
                Sum_pdnNT(ref gnhltm, ref gn0hl, "new_hopdongdautummtb", hdRef, -1, "new_nghiemthummtb", NT.Id);

                #endregion                
            }
            
            Entity tmpPdk = new Entity(pdkRef.LogicalName);
            tmpPdk.Id = pdkRef.Id;
            tmpPdk["new_dinhmuc_hoanlai_tienmat"] = new Money(dmhl - dmhlvt);
            tmpPdk["new_dinhmuc_hoanlai_vattu"] = new Money(dmhlvt);
            tmpPdk["new_dinhmuc_khonghoanlai"] = new Money(dm0hl);
            //-------------------------------------------------------

            tmpPdk["new_giaingan_hoanlai_tienmat"] = new Money(gnhltm);
            tmpPdk["new_giaingan_hoanlai_vattu"] = new Money(gnhlvt);
            tmpPdk["new_giaingan_khonghoanlai"] = new Money(gn0hl);
            //-------------------------------------------------------

            decimal deNghiTmHl = dmhl - dmhlvt - gnhltm;
            decimal deNghiVt = dmhlvt - gnhlvt;
            decimal deNghiKhl = dm0hl - gn0hl;

            tmpPdk["new_dinhmucchi_hoanlai_tienmat"] = new Money(deNghiTmHl);
            tmpPdk["new_dinhmucchi_hoanlai_vattu"] = new Money(deNghiVt);
            tmpPdk["new_dinhmucchi_khonghoanlai"] = new Money(deNghiKhl);

            tmpPdk["new_denghi_hoanlai_tienmat"] = new Money(deNghiTmHl);

            if (!tmpPdk.Contains("new_denghi_hoanlai_tienmat"))
                tmpPdk["new_denghi_hoanlai_tienmat"] = new Money(0);
            if (!tmpPdk.Contains("new_denghi_hoanlai_vattu"))
                tmpPdk["new_denghi_hoanlai_vattu"] = new Money(0);
            if (!tmpPdk.Contains("new_denghi_khonghoanlai"))
                tmpPdk["new_denghi_khonghoanlai"] = new Money(0);

            service.Update(tmpPdk);
        }

        private void sum_pdk(EntityReference hd, EntityReference pdkRef, ref decimal hlTM, ref decimal hlVT, ref decimal KHL)
        {
            decimal sumhlTM = 0;
            decimal sumhlVT = 0;
            decimal sumKHL = 0;

            #region sub
            //dang ky hom giong
            StringBuilder fetch = new StringBuilder();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkyhomgiong'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkyhomgiong")
                fetch.AppendFormat("<condition attribute='new_phieudangkyhomgiongid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky phan bon
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkyphanbon'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkyphanbon")
                fetch.AppendFormat("<condition attribute='new_phieudangkyphanbonid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky thuoc
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkythuoc'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");

            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkythuoc")
                fetch.AppendFormat("<condition attribute='new_phieudangkythuocid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");

            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky vat tu
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkyvattu'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");

            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkyvattu")
                fetch.AppendFormat("<condition attribute='new_phieudangkyvattuid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky dich vu
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkydichvu'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");

            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkydichvu")
                fetch.AppendFormat("<condition attribute='new_phieudangkydichvuid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");

            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }
            #endregion

            hlTM += sumhlTM;
            hlVT += sumhlVT;
            KHL += sumKHL;
        }

        private void Sum_pdn(ref decimal tm, ref decimal khl, EntityReference hdRef, Guid pdngn)
        {
            decimal sum_tm = 0;
            decimal sum_khl = 0;
            StringBuilder fetch = new StringBuilder();

            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_chitietphieudenghigiaingan'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hl' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<link-entity name='new_phieudenghigiaingan' from='new_phieudenghigiainganid' to='new_phieudenghigiaingan' alias='pdn' link-type='inner'>");
            fetch.AppendFormat("<attribute name='statuscode' groupby='true' alias='statuscode' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hdRef.Id);
            fetch.AppendFormat("<condition attribute='new_phieudenghigiainganid' operator='neq' value='{0}'/>", pdngn);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</link-entity>");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_noidunggiaingan' operator='eq' value='100000000'/>");
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));

            foreach (Entity ct in etnc.Entities)
            {
                AliasedValue ahl = ct.Contains("hl") ? (AliasedValue)ct["hl"] : null;
                if (ahl != null)
                    sum_tm += ahl.Value != null ? ((Money)ahl.Value).Value : 0;

                AliasedValue ahlvt = ct.Contains("hlvt") ? (AliasedValue)ct["hlvt"] : null;
                if (ahlvt != null)
                    sum_tm += ahlvt.Value != null ? ((Money)ahlvt.Value).Value : 0;

                AliasedValue akhl = ct.Contains("khl") ? (AliasedValue)ct["khl"] : null;
                if (akhl != null)
                    sum_khl += akhl.Value != null ? ((Money)akhl.Value).Value : 0;
            }
            tm += sum_tm;
            khl += sum_khl;
        }

        private void sum_current_ct_pdk(EntityReference pdkRef, ref decimal hl, ref decimal khl)
        {
            decimal tmpHl = 0;
            decimal tmp0hl = 0;
            StringBuilder fetch = new StringBuilder();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_chitietdangkyphanbon'>");
            fetch.AppendFormat("<attribute name='new_sotienkhl' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_sotienhl' alias='hl' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_phieudangkyphanbon' operator='eq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hl"))
                {
                    AliasedValue als = (AliasedValue)tmp["hl"];
                    if (als.Value != null)
                        tmpHl = ((Money)((AliasedValue)tmp["hl"]).Value).Value;
                }

                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        tmp0hl = ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }

            }
            hl = tmpHl;
            khl = tmp0hl;
        }

        class Tmp
        {
            public string new_machinhsach = "";
            public decimal new_dinhmucdautukhonghoanlai = 0;
            public decimal new_dinhmuctamung = 0;
            public decimal new_dinhmucdautuhoanlai = 0;
            public decimal new_dinhmucphanbontoithieu = 0;
            public int new_mucdichdautu = -1;
            public DataCollection<Entity> gns = null;
        }

        private decimal GetTyle(Guid chinhsach, int ttNT, bool yeucau)
        {
            decimal tyle = 0;

            QueryExpression qe = new QueryExpression("new_dinhmucdautu");
            qe.ColumnSet = new ColumnSet(true);
            qe.Criteria.AddCondition(new ConditionExpression("new_yeucau", ConditionOperator.LessEqual, ttNT));
            qe.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal, chinhsach));

            foreach (Entity a in service.RetrieveMultiple(qe).Entities)
            {
                if (!yeucau)
                    tyle += (decimal)a["new_phantramtilegiaingan"];
                else
                {
                    tyle += (decimal)a["new_tyleyc"];
                }
            }

            return tyle;
        }

        private void sum_pdkNT(EntityReference hd, EntityReference pdkRef, ref decimal hlTM, ref decimal hlVT, ref decimal KHL, int type, string fieldNT, Guid pNT)
        {
            decimal sumhlTM = 0;
            decimal sumhlVT = 0;
            decimal sumKHL = 0;
            #region sub
            //dang ky hom giong
            StringBuilder fetch = new StringBuilder();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkyhomgiong'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkyhomgiong")
                fetch.AppendFormat("<condition attribute='new_phieudangkyhomgiongid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("<condition attribute='new_loainghiemthu' operator='eq' value='{0}'/>", type);
            fetch.AppendFormat("<condition attribute='" + fieldNT + "' operator='eq' value='{0}'/>", pNT);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }
            //dang ky phan bon
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkyphanbon'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkyphanbon")
                fetch.AppendFormat("<condition attribute='new_phieudangkyphanbonid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("<condition attribute='new_loainghiemthu' operator='eq' value='{0}'/>", type);
            fetch.AppendFormat("<condition attribute='" + fieldNT + "' operator='eq' value='{0}'/>", pNT);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky thuoc
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkythuoc'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");

            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkythuoc")
                fetch.AppendFormat("<condition attribute='new_phieudangkythuocid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("<condition attribute='new_loainghiemthu' operator='eq' value='{0}'/>", type);
            fetch.AppendFormat("<condition attribute='" + fieldNT + "' operator='eq' value='{0}'/>", pNT);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky vat tu
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkyvattu'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");

            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkyvattu")
                fetch.AppendFormat("<condition attribute='new_phieudangkyvattuid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("<condition attribute='new_loainghiemthu' operator='eq' value='{0}'/>", type);
            fetch.AppendFormat("<condition attribute='" + fieldNT + "' operator='eq' value='{0}'/>", pNT);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }

            //dang ky dich vu
            fetch.Clear();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_phieudangkydichvu'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hlTm' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");

            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'/>", hd.Id);
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            if (pdkRef.LogicalName == "new_phieudangkydichvu")
                fetch.AppendFormat("<condition attribute='new_phieudangkydichvuid' operator='neq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("<condition attribute='new_loainghiemthu' operator='eq' value='{0}'/>", type);
            fetch.AppendFormat("<condition attribute='" + fieldNT + "' operator='eq' value='{0}'/>", pNT);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hlTm"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlTm"];
                    if (als.Value != null)
                        sumhlTM += ((Money)((AliasedValue)tmp["hlTm"]).Value).Value;
                }
                if (tmp.Contains("hlvt"))
                {
                    AliasedValue als = (AliasedValue)tmp["hlvt"];
                    if (als.Value != null)
                        sumhlVT += ((Money)((AliasedValue)tmp["hlvt"]).Value).Value;
                }
                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        sumKHL += ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }
            }
            #endregion

            hlTM += sumhlTM;
            hlVT += sumhlVT;
            KHL += sumKHL;
        }

        private void Sum_pdnNT(ref decimal tm, ref decimal khl, string field, EntityReference hdRef, int type, string fieldNT, Guid pNT)
        {
            decimal sum_tm = 0;
            decimal sum_khl = 0;
            StringBuilder fetch = new StringBuilder();

            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_chitietphieudenghigiaingan'>");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_tienmat' alias='hl' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_hoanlai_vattu' alias='hlvt' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_denghi_khonghoanlai' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<link-entity name='new_phieudenghigiaingan' from='new_phieudenghigiainganid' to='new_phieudenghigiaingan' alias='pdn' link-type='inner'>");
            fetch.AppendFormat("<attribute name='statuscode' groupby='true' alias='statuscode' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='statuscode' operator='eq' value='100000000'/>");
            fetch.AppendFormat("<condition attribute='" + field + "' operator='eq' value='{0}'/>", hdRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</link-entity>");
            fetch.AppendFormat("<filter type='and'>");
            if (type != -1)
                fetch.AppendFormat("<condition attribute='new_noidunggiaingan' operator='eq' value='{0}'/>", type + 1);
            fetch.AppendFormat("<condition attribute='" + fieldNT + "' operator='eq' value='{0}'/>", pNT);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            foreach (Entity ct in etnc.Entities)
            {
                AliasedValue ahl = ct.Contains("hl") ? (AliasedValue)ct["hl"] : null;
                if (ahl != null)
                    sum_tm += ahl.Value != null ? ((Money)ahl.Value).Value : 0;

                AliasedValue ahlvt = ct.Contains("hlvt") ? (AliasedValue)ct["hlvt"] : null;
                if (ahlvt != null)
                    sum_tm += ahlvt.Value != null ? ((Money)ahlvt.Value).Value : 0;

                AliasedValue akhl = ct.Contains("khl") ? (AliasedValue)ct["khl"] : null;
                if (akhl != null)
                    sum_khl += akhl.Value != null ? ((Money)akhl.Value).Value : 0;
            }
            tm += sum_tm;
            khl += sum_khl;
        }

        private bool CheckDuplicateGuid(List<Guid> temps, Guid t)
        {
            int n = temps.Count;

            if (n < 1)
                return true;
            else
            {

                foreach (Guid en in temps)
                {
                    if (en.ToString() == t.ToString())
                    {
                        return false;
                    }
                }

                return true;
            }
        }
    }
}
