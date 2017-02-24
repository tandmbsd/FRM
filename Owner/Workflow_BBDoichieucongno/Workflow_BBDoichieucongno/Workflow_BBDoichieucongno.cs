using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Workflow;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;
using System.Activities;

namespace Workflow_BBDoichieucongno
{
    public class Workflow_BBDoichieucongno : CodeActivity
    {
        [RequiredArgument]
        [Input("InputEntity")]
        [ReferenceTarget("new_bienbandoichieucongno")]
        public InArgument<EntityReference> inputEntity { get; set; }
        public IOrganizationService service;
        public ITracingService tracingService;
        List<Phanbodautu> phanbodautus = new List<Phanbodautu>();
        Guid enId = new Guid();

        private decimal tongtien = 0;
        private decimal tonglai = 0;
        private int flag = 0;
        private DateTime ngaythu = DateTime.Now;
        private int hinhthuctanglaisuatthaydoi;
        private decimal gocthat = 0;

        protected override void Execute(CodeActivityContext executionContext)
        {
            #region initial and get input

            tracingService = executionContext.GetExtension<ITracingService>();
            IWorkflowContext context = executionContext.GetExtension<IWorkflowContext>();
            IOrganizationServiceFactory serviceFactory = executionContext.GetExtension<IOrganizationServiceFactory>();
            service = serviceFactory.CreateOrganizationService(context.UserId);

            enId = this.inputEntity.Get(executionContext).Id;
            EntityReference bbdccnErf = this.inputEntity.Get(executionContext);
            Entity BBdoichieucongno = service.Retrieve(bbdccnErf.LogicalName, bbdccnErf.Id, new ColumnSet(true));
            decimal tragoc = 0;

            #endregion

            #region check contain and get data

            if (!BBdoichieucongno.Contains("new_vuhientai"))
                throw new Exception("Biên bản không có vụ đầu tư");

            if (!BBdoichieucongno.Contains("new_ngaylap"))
                throw new Exception("Biên bản không có ngày lập");

            if (!BBdoichieucongno.Contains("new_thoidiemtinh"))
                throw new Exception("Biên bản không có thời điểm tính");

            if (!BBdoichieucongno.Contains("new_khachhang") && !BBdoichieucongno.Contains("new_khachhangdoanhnghiep"))
                throw new Exception("Biên bản không có khách hàng");

            Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)BBdoichieucongno["new_vuhientai"]).Id,
                new ColumnSet(true));

            hinhthuctanglaisuatthaydoi = ((OptionSetValue)vudautu["new_hinhthuctanglai"]).Value;
            DateTime ngaylap = (DateTime)BBdoichieucongno["new_ngaylap"];
            DateTime thoidiemtinh = (DateTime)BBdoichieucongno["new_thoidiemtinh"];

            #endregion

            #region get list pbdt

            QueryExpression q = new QueryExpression("new_phanbodautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression(LogicalOperator.And);

            q.Criteria.AddCondition(new ConditionExpression("new_ngayphatsinh", ConditionOperator.GreaterEqual, ngaylap.ToString("O")));
            q.Criteria.AddCondition(new ConditionExpression("new_ngayphatsinh", ConditionOperator.LessEqual, thoidiemtinh.ToString("O")));

            FilterExpression f1 = new FilterExpression(LogicalOperator.Or);
            f1.AddCondition(new ConditionExpression("new_conlai", ConditionOperator.Null));
            f1.AddCondition(new ConditionExpression("new_conlai", ConditionOperator.GreaterThan, decimal.Zero));

            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautu.Id));

            if (BBdoichieucongno.Contains("new_khachhang"))
                q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal,
                    ((EntityReference)BBdoichieucongno["new_khachhang"]).Id));
            else if (BBdoichieucongno.Contains("new_khachhangdoanhnghiep"))
                q.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal,
                    ((EntityReference)BBdoichieucongno["new_khachhangdoanhnghiep"]).Id));

            q.Criteria.AddFilter(f1);
            q.AddOrder("new_ngayphatsinh", OrderType.Ascending);
            q.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);

            EntityCollection entc = service.RetrieveMultiple(q);

            #endregion

            #region tinh lai va gen phieu tinh lai

            foreach (Entity en in entc.Entities)
            {
                phanbodautus.Add(CreatePBDT(en.Id));
                tragoc += en.Contains("new_conlai") ? ((Money)en["new_conlai"]).Value : 0;
            }

            if (phanbodautus.Count > 0 && tragoc > 0)
                Tragoc_Laitrentienthu(phanbodautus, tragoc, BBdoichieucongno);

            #endregion

        }

        private void Tragoc_Laitrentienthu(List<Phanbodautu> lstPbdt, decimal tragoc, Entity target)
        {
            ClearAllPhieutinhlai();
            decimal Totallai = 0;
            decimal TongtienScope = 0;
            flag = 1;
            decimal tonggoc = 0;
            DateTime bg = new DateTime();

            foreach (Phanbodautu pbdt in lstPbdt)
            {
                if (pbdt.sotien == 0 || pbdt.loailaisuat == 0)
                    continue;

                gocthat = 0;
                bg = pbdt.ngayphatsinh;

                if (TongtienScope >= tragoc)
                    break;

                decimal sotientinhlai = pbdt.sotien > (tragoc - TongtienScope) ? (tragoc - TongtienScope) : pbdt.sotien;
                TongtienScope += sotientinhlai;
                gocthat = sotientinhlai;

                decimal tl = Tinhlaitungphieuphanbo(pbdt, sotientinhlai, bg, ngaythu, flag);
                Totallai += tl;
                tonggoc += sotientinhlai;
            }

            tongtien = tragoc + Totallai;
            tonglai = Totallai;

            Entity targetUpdate = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_tonglai", "new_tongnogoc" }));

            targetUpdate["new_tonglai"] = new Money(Totallai);
            targetUpdate["new_tongnogoc"] = new Money(tragoc);

            service.Update(targetUpdate);
        }

        private void ClearAllPhieutinhlai()
        {
            List<Entity> lstPhieutinhlai = RetrieveMultiRecord(service, "new_phieutinhlai",
                new ColumnSet(new string[] { "new_phieutinhlaiid" }), "new_bienbandoichieucongno", enId);
            //throw new Exception(lstPhieutinhlai.Entities.Count.ToString());
            EntityReferenceCollection t = new EntityReferenceCollection();

            foreach (Entity a in lstPhieutinhlai)
            {
                t.Add(a.ToEntityReference());

                List<Entity> lstDoantinhlai = RetrieveMultiRecord(service, "new_doantinhlai",
                    new ColumnSet(new string[] { "new_doantinhlaiid" }), "new_phieutinhlai", a.Id);

                foreach (Entity dtl in lstDoantinhlai)
                    service.Delete(dtl.LogicalName, dtl.Id);

            }

            foreach (Entity en in lstPhieutinhlai)
                service.Delete(en.LogicalName, en.Id);

        }

        private decimal Tinhlaitungphieuphanbo(Phanbodautu pbdt, decimal sotien, DateTime begin, DateTime end, int flag)
        {
            decimal result = 0;
            decimal laibandau = 0;
            Dictionary<Guid, List<phieutinhlai>> ls = new Dictionary<Guid, List<phieutinhlai>>();

            if (!ls.ContainsKey(pbdt.ID))
                ls.Add(pbdt.ID, new List<phieutinhlai>());

            if (pbdt.loailaisuat == 100000000) // co dinh
            {
                #region loai suat co dinh

                result = (decimal)end.Date.Subtract(begin.Date).TotalDays * (pbdt.laisuat / 3000) * sotien;

                if (flag == 1) // hinh thuc tra goc
                {
                    result += sotien;
                    ls[pbdt.ID].Add(new phieutinhlai
                        (
                            (int)end.Date.Subtract(begin.Date).TotalDays,
                            sotien,
                            pbdt.laisuat,
                            begin,
                            end,
                            pbdt.laisuat,
                            flag == -1 ? 1 : 0
                        ));
                }
                else if (flag == 2) // hinh thuc tra tong tien
                {
                    if (gocthat > 0)
                    {
                        result += sotien;
                        ls[pbdt.ID].Add(new phieutinhlai
                        (
                            (int)end.Date.Subtract(begin.Date).TotalDays,
                            sotien,
                            pbdt.laisuat,
                            begin,
                            end,
                            pbdt.laisuat,
                            flag == -1 ? 1 : 0
                        ));
                    }
                }

                #endregion

            }
            else if (pbdt.loailaisuat == 100000001) // thay doi
            {
                #region retrieve bang lai

                QueryExpression qbangLai = new QueryExpression("new_banglaisuatthaydoi");
                qbangLai.ColumnSet = new ColumnSet(new string[] { "new_name", "new_ngayapdung", "new_phantramlaisuat", "new_ma" });
                qbangLai.Criteria = new FilterExpression(LogicalOperator.And);
                qbangLai.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                qbangLai.Criteria.AddCondition(new ConditionExpression("new_vudautuapdung", ConditionOperator.Equal, pbdt.vudautu.Id));

                if (pbdt.loaihopdong == 100000000) // hop dong mia
                {
                    FilterExpression hopdongmia = new FilterExpression(LogicalOperator.Or);
                    hopdongmia.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000000")); // trong va cham soc mia
                    hopdongmia.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000002")); // tuoi mia
                    hopdongmia.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000003")); // boc la mia
                    qbangLai.Criteria.AddFilter(hopdongmia);
                }

                else if (pbdt.loaihopdong == 100000001) // thue dat
                {
                    FilterExpression hdthuedat = new FilterExpression(LogicalOperator.Or);
                    hdthuedat.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000005")); // thue dat
                    qbangLai.Criteria.AddFilter(hdthuedat);
                }
                else if (pbdt.loaihopdong == 100000002) // trang thiet bi
                {
                    FilterExpression hdtrangthietbi = new FilterExpression(LogicalOperator.Or);
                    hdtrangthietbi.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000006")); // mua may moc thiet bi
                    qbangLai.Criteria.AddFilter(hdtrangthietbi);
                }
                else if (pbdt.loaihopdong == 100000003) // hd ha tang
                {
                    FilterExpression hdhatang = new FilterExpression(LogicalOperator.Or);
                    hdhatang.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, "100000007")); // dau tu ha tang
                    qbangLai.Criteria.AddFilter(hdhatang);
                }

                qbangLai.AddOrder("new_ngayapdung", OrderType.Ascending);
                EntityCollection bls = service.RetrieveMultiple(qbangLai);

                #endregion

                bool isBefore = false;

                if (bls.Entities.Count <= 0)
                {
                    result = Math.Round((decimal)end.Date.Subtract(begin.Date).TotalDays * (pbdt.laisuat / 3000) * sotien,
                        MidpointRounding.AwayFromZero);

                    if (flag == 1) // hinh thuc tra goc
                    {
                        result += sotien;
                        ls[pbdt.ID].Add(new phieutinhlai
                        (
                            (int)end.Date.Subtract(begin.Date).TotalDays,
                            sotien,
                            pbdt.laisuat,
                            begin,
                            end,
                            pbdt.laisuat,
                            flag == -1 ? 1 : 0
                        ));
                    }
                    else if (flag == 2) // hinh thuc tra tong tien
                    {
                        if (gocthat > 0)
                        {
                            result += sotien;
                            ls[pbdt.ID].Add(new phieutinhlai
                        (
                            (int)end.Date.Subtract(begin.Date).TotalDays,
                            sotien,
                            pbdt.laisuat,
                            begin,
                            end,
                            pbdt.laisuat,
                            flag == -1 ? 1 : 0
                        ));
                        }
                    }
                }
                else
                {
                    if (hinhthuctanglaisuatthaydoi == 100000000) // lay muc lai nho nhat 
                    {
                        #region hinh thuc tang lai suat la lay muc lai nho nhat

                        decimal tmpsn = CompareDate(begin, end);

                        if (tmpsn > 0)
                            throw new Exception("Ngày tính lãi phải lớn hơn hoặc bằng ngày phát sinh hoặc ngày sau cùng tính lãi!");
                        else if (tmpsn == 0)
                            return result;
                        else
                        {
                            #region Find begin & end point
                            point pt1 = null;
                            point pt2 = null;
                            int tmpI = 0;

                            for (int i = 0; i < bls.Entities.Count; i++)
                            {
                                Entity etmp = bls[i];

                                if (!etmp.Contains("new_ngayapdung"))
                                    throw new Exception(string.Format("Vui lòng nhập ngày áp dụng cho bảng lãi {0} trên chính sách đầu tư!", etmp["new_name"]));
                                if (!etmp.Contains("new_phantramlaisuat"))
                                    throw new Exception(string.Format("Vui lòng nhập lãi suất cho bảng lãi {0} trên chính sách đầu tư!", etmp["new_name"]));
                                DateTime tmp = ((DateTime)etmp["new_ngayapdung"]);

                                if (pt1 == null)
                                {
                                    if (i == 0 && begin.Date < tmp.Date)
                                    {
                                        pt1 = new point();
                                        pt1.index = -1;
                                        pt1.sn = (int)((DateTime)bls[0]["new_ngayapdung"]).Date.Subtract(begin.Date).TotalDays;

                                        pt1.isBefore = true;
                                        isBefore = true;
                                    }
                                    else if (begin.Date < tmp.Date)
                                    {
                                        pt1 = new point();
                                        pt1.index = tmpI;
                                        pt1.sn = (int)begin.Date.Subtract(((DateTime)bls[tmpI]["new_ngayapdung"]).Date).TotalDays;
                                    }
                                    else if (begin.Date == tmp.Date)
                                    {
                                        pt1 = new point();
                                        pt1.index = i;
                                        pt1.sn = 0;
                                    }
                                }

                                if (pt2 == null)
                                {
                                    if (end.Date < tmp.Date)
                                    {
                                        pt2 = new point();
                                        pt2.index = tmpI;
                                        pt2.sn = (int)end.Date.Subtract(((DateTime)bls[tmpI]["new_ngayapdung"]).Date).TotalDays;

                                        if (pt1.isBefore == true)
                                            pt2.index++;
                                    }
                                    else if (end.Date == tmp.Date)
                                    {
                                        pt2 = new point();
                                        pt2.index = i;
                                        pt2.sn = 0;

                                        if (pt1.isBefore == true)
                                            pt2.index++;
                                    }
                                    else
                                    {
                                        if (i == bls.Entities.Count - 1)
                                        {
                                            pt2 = new point();
                                            pt2.index = i;
                                            pt2.sn = (int)end.Date.Subtract(tmp.Date).TotalDays;
                                            pt2.isOver = true;

                                        }
                                    }
                                }

                                if (pt1 != null && pt2 != null)
                                    break;
                                tmpI = i;
                            }
                            #endregion
                            
                            laibandau = isBefore == true ? pbdt.laisuat : (decimal)bls[0]["new_phantramlaisuat"];

                            #region (pt1 == null || pt2 == null) 

                            if (pt1 == null || pt2 == null)
                            {
                                Entity etmp = bls[bls.Entities.Count - 1];

                                int sn = (int)CompareDate(end, begin);
                                decimal pLs = (decimal)etmp["new_phantramlaisuat"];
                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    begin,
                                    end,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));

                                if (pbdt.status == 1 && flag != -1)
                                {
                                    #region Create PTL
                                    Guid id = Guid.Empty;
                                    Createphieutinhlai(pbdt, sotien, result, ref id);
                                    pbdt.status = 0;
                                    var sorted = ls[pbdt.ID].OrderBy(p => p.to).ToList();

                                    foreach (phieutinhlai o in sorted)
                                    {
                                        if (o.status == 0)
                                            Createdoantinhlai(id, o.fr, o.to, o.ls, o.tl, o.sotien, o.sn);
                                    }
                                    #endregion
                                }

                                return result;
                            }

                            #endregion

                            #region (pt1.index == pt2.index)

                            if (pt1.index == pt2.index)
                            {
                                Entity etmp = bls[pt1.index];
                                laibandau = (decimal)bls[pt1.index]["new_phantramlaisuat"];
                                DateTime tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);
                                DateTime bg = tmp.Date.AddDays(pt1.sn);
                                DateTime ed = tmp.Date.AddDays(pt2.sn);

                                int sn = (int)CompareDate(ed, bg);
                                decimal pLs = (decimal)etmp["new_phantramlaisuat"];
                                decimal tl = Math.Round((decimal)(sn * (pLs > laibandau ? laibandau : pLs) * sotien / 3000), MidpointRounding.AwayFromZero);

                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    bg,
                                    ed,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));
                            }

                            #endregion

                            else if (pt2.index - pt1.index == 1)
                            {
                                laibandau = (decimal)bls[pt1.index]["new_phantramlaisuat"];
                                Entity etmp1 = bls[pt1.index];
                                Entity etmp2 = bls[pt2.index];

                                DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                DateTime tmp2 = (((DateTime)etmp2["new_ngayapdung"])).AddHours(7);

                                DateTime bg = tmp1.Date.AddDays(pt1.sn);
                                int sn = (int)CompareDate(tmp2, bg);
                                decimal pLs = (decimal)etmp1["new_phantramlaisuat"];
                                pLs = pLs > laibandau ? laibandau : pLs;

                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    bg,
                                    tmp2.Date,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));

                                DateTime ed = tmp2.Date.AddDays(pt2.sn);
                                sn = (int)CompareDate(ed, tmp2);
                                pLs = (decimal)etmp2["new_phantramlaisuat"];
                                pLs = pLs > laibandau ? laibandau : pLs;
                                tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    tmp2.Date,
                                    ed,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));

                            }
                            else
                            {
                                Entity cEtn = isBefore == true ? bls[0] : bls[pt1.index];
                                DateTime cDate = cEtn == null ? new DateTime() : (((DateTime)cEtn["new_ngayapdung"])).AddHours(7);

                                if (isBefore == true)
                                {
                                    #region isbefore true

                                    for (int i = pt1.index; i < pt2.index + 1; i++)
                                    {
                                        Entity etmp = null;
                                        DateTime tmp = new DateTime();

                                        if (i == -1)
                                        {
                                            tmp = begin;
                                        }
                                        else
                                        {
                                            etmp = bls[i];
                                            tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);
                                        }

                                        if (i == pt1.index)
                                        {
                                            if (i == -1)
                                            {
                                                i++;
                                                Entity etmp1 = bls[i];
                                                DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                                DateTime bg = begin;

                                                int sn = (int)CompareDate(tmp1, bg);
                                                decimal pLs = pbdt.laisuat;
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    bg,
                                                    tmp1.Date,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp1;
                                                cEtn = etmp1;

                                            }
                                            else
                                            {
                                                #region
                                                i++;
                                                Entity etmp1 = bls[i];
                                                DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                                DateTime bg = tmp.Date.AddDays(pt1.sn);

                                                int sn = (int)CompareDate(tmp1, bg);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    bg,
                                                    tmp1.Date,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp1;
                                                cEtn = etmp1;
                                                #endregion
                                            }
                                        }
                                        else if (i != pt2.index)
                                        {
                                            //ls.Add((int)tmp.Date.Subtract(cDate.Date).TotalDays);
                                            int sn = (int)CompareDate(tmp, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls[pbdt.ID].Add(new phieutinhlai
                                            (
                                                sn,
                                                sotien,
                                                pLs,
                                                cDate.Date,
                                                tmp.Date,
                                                tl,
                                                flag == -1 ? 1 : 0
                                            ));
                                            cDate = tmp;
                                            cEtn = etmp;
                                        }
                                        else if (i == pt2.index)
                                        {
                                            if (pt2.isOver == true)
                                            {
                                                int snTmpI = (int)tmp.Date.Subtract(cDate.Date).TotalDays;
                                                decimal pLsTmpI = (decimal)cEtn["new_phantramlaisuat"];
                                                pLsTmpI = pLsTmpI > laibandau ? laibandau : pLsTmpI;
                                                decimal tlTmpI = Math.Round((decimal)(snTmpI * pLsTmpI * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tlTmpI;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    snTmpI,
                                                    sotien,
                                                    pLsTmpI,
                                                    cDate,
                                                    tmp.Date,
                                                    tlTmpI,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp;
                                                cEtn = etmp;

                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            else
                                            {
                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            break;
                                        }
                                    }

                                    #endregion

                                }
                                else
                                {
                                    for (int i = pt1.index; i < pt2.index + 1; i++)
                                    {
                                        Entity etmp = bls[i];
                                        DateTime tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);

                                        if (i == pt1.index)
                                        {
                                            i++;
                                            Entity etmp1 = bls[i];
                                            DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                            DateTime bg = tmp.Date.AddDays(pt1.sn);

                                            int sn = (int)CompareDate(tmp1, bg);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                            result += tl;
                                            ls[pbdt.ID].Add(new phieutinhlai
                                            (
                                                sn,
                                                sotien,
                                                pLs,
                                                bg,
                                                tmp1.Date,
                                                tl,
                                                flag == -1 ? 1 : 0
                                            ));
                                            cDate = tmp1;
                                            cEtn = etmp1;

                                        }
                                        else if (i != pt2.index)
                                        {
                                            //ls.Add((int)tmp.Date.Subtract(cDate.Date).TotalDays);
                                            int sn = (int)CompareDate(tmp, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls[pbdt.ID].Add(new phieutinhlai
                                            (
                                                sn,
                                                sotien,
                                                pLs,
                                                cDate.Date,
                                                tmp.Date,
                                                tl,
                                                flag == -1 ? 1 : 0
                                            ));
                                            cDate = tmp;
                                            cEtn = etmp;
                                        }
                                        else if (i == pt2.index)
                                        {
                                            if (pt2.isOver == true)
                                            {
                                                int snTmpI = (int)tmp.Date.Subtract(cDate.Date).TotalDays;
                                                decimal pLsTmpI = (decimal)cEtn["new_phantramlaisuat"];
                                                pLsTmpI = pLsTmpI > laibandau ? laibandau : pLsTmpI;
                                                decimal tlTmpI = Math.Round((decimal)(snTmpI * pLsTmpI * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tlTmpI;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    snTmpI,
                                                    sotien,
                                                    pLsTmpI,
                                                    cDate,
                                                    tmp.Date,
                                                    tlTmpI,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp;
                                                cEtn = etmp;

                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            else
                                            {
                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                            
                            if (pbdt.status == 1 && flag != -1)
                            {
                                #region Create PTL
                                Guid id = Guid.Empty;
                                Createphieutinhlai(pbdt, sotien, result, ref id);
                                pbdt.status = 0;
                                var sorted = ls[pbdt.ID].OrderBy(p => p.to).ToList();

                                foreach (phieutinhlai o in sorted)
                                {
                                    if (o.status == 0)
                                        Createdoantinhlai(id, o.fr, o.to, o.ls, o.tl, o.sotien, o.sn);
                                }
                                #endregion
                            }

                            return result;
                        }

                        #endregion
                    }
                    else // lay theo bang lai suat
                    {
                        #region lay theo bang lai

                        decimal tmpsn = CompareDate(begin, end);

                        if (tmpsn > 0)
                            throw new Exception("Ngày tính lãi phải lớn hơn hoặc bằng ngày phát sinh hoặc ngày sau cùng tính lãi!");
                        else if (tmpsn == 0)
                            return result;
                        else
                        {
                            #region Find begin & end point
                            point pt1 = null;
                            point pt2 = null;
                            int tmpI = 0;

                            for (int i = 0; i < bls.Entities.Count; i++)
                            {
                                Entity etmp = bls[i];

                                if (!etmp.Contains("new_ngayapdung"))
                                    throw new Exception(string.Format("Vui lòng nhập ngày áp dụng cho bảng lãi {0} trên chính sách đầu tư!", etmp["new_name"]));
                                if (!etmp.Contains("new_phantramlaisuat"))
                                    throw new Exception(string.Format("Vui lòng nhập lãi suất cho bảng lãi {0} trên chính sách đầu tư!", etmp["new_name"]));
                                DateTime tmp = ((DateTime)etmp["new_ngayapdung"]);

                                if (pt1 == null)
                                {
                                    //if (i == 0 && begin.Date < tmp.Date)
                                    //    throw new Exception(string.Format("Ngày áp dụng {0:dd-MM-yyyy} không có trên bảng lãi thay đổi. Vui lòng thêm vào!", begin.Date));
                                    if (i == 0 && begin.Date < tmp.Date)
                                    {
                                        pt1 = new point();
                                        pt1.index = -1;
                                        pt1.sn = (int)((DateTime)bls[0]["new_ngayapdung"]).Date.Subtract(begin.Date).TotalDays;

                                        pt1.isBefore = true;
                                        isBefore = true;
                                    }
                                    else if (begin.Date < tmp.Date)
                                    {
                                        pt1 = new point();
                                        pt1.index = tmpI;
                                        pt1.sn = (int)begin.Date.Subtract(((DateTime)bls[tmpI]["new_ngayapdung"]).Date).TotalDays;
                                    }
                                    else if (begin.Date == tmp.Date)
                                    {
                                        pt1 = new point();
                                        pt1.index = i;
                                        pt1.sn = 0;
                                    }
                                }

                                if (pt2 == null)
                                {
                                    if (end.Date < tmp.Date)
                                    {
                                        pt2 = new point();
                                        pt2.index = tmpI;
                                        pt2.sn = (int)end.Date.Subtract(((DateTime)bls[tmpI]["new_ngayapdung"]).Date).TotalDays;

                                        if (pt1.isBefore == true)
                                            pt2.index++;
                                    }
                                    else if (end.Date == tmp.Date)
                                    {
                                        pt2 = new point();
                                        pt2.index = i;
                                        pt2.sn = 0;

                                        if (pt1.isBefore == true)
                                            pt2.index++;
                                    }
                                    else
                                    {
                                        if (i == bls.Entities.Count - 1)
                                        {
                                            pt2 = new point();
                                            pt2.index = i;
                                            pt2.sn = (int)end.Date.Subtract(tmp.Date).TotalDays;
                                            pt2.isOver = true;

                                            //if (isBefore == true)
                                            //    pt2.index++;

                                        }
                                    }
                                }

                                if (pt1 != null && pt2 != null)
                                    break;
                                tmpI = i;
                            }
                            #endregion

                            laibandau = isBefore == true ? pbdt.laisuat : (decimal)bls[0]["new_phantramlaisuat"];

                            if (pt1 == null || pt2 == null)
                            {
                                Entity etmp = bls[bls.Entities.Count - 1];

                                int sn = (int)CompareDate(end, begin);
                                decimal pLs = (decimal)etmp["new_phantramlaisuat"];
                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    begin,
                                    end,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));

                                return result;
                            }

                            if (pt1.index == pt2.index)
                            {
                                Entity etmp = bls[pt1.index];
                                laibandau = (decimal)bls[pt1.index]["new_phantramlaisuat"];
                                DateTime tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);
                                DateTime bg = tmp.Date.AddDays(pt1.sn);
                                DateTime ed = tmp.Date.AddDays(pt2.sn);

                                int sn = (int)CompareDate(ed, bg);
                                decimal pLs = (decimal)etmp["new_phantramlaisuat"];
                                decimal tl = Math.Round((decimal)(sn * (pLs > laibandau ? laibandau : pLs) * sotien / 3000), MidpointRounding.AwayFromZero);

                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    bg,
                                    ed,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));
                            }
                            else if (pt2.index - pt1.index == 1)
                            {
                                laibandau = (decimal)bls[pt1.index]["new_phantramlaisuat"];
                                Entity etmp1 = bls[pt1.index];
                                Entity etmp2 = bls[pt2.index];

                                DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                DateTime tmp2 = (((DateTime)etmp2["new_ngayapdung"])).AddHours(7);

                                DateTime bg = tmp1.Date.AddDays(pt1.sn);
                                int sn = (int)CompareDate(tmp2, bg);
                                decimal pLs = (decimal)etmp1["new_phantramlaisuat"];
                                pLs = pLs > laibandau ? laibandau : pLs;

                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    bg,
                                    tmp2.Date,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));

                                DateTime ed = tmp2.Date.AddDays(pt2.sn);
                                sn = (int)CompareDate(ed, tmp2);
                                pLs = (decimal)etmp2["new_phantramlaisuat"];
                                pLs = pLs > laibandau ? laibandau : pLs;
                                tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                result += tl;
                                ls[pbdt.ID].Add(new phieutinhlai
                                (
                                    sn,
                                    sotien,
                                    pLs,
                                    tmp2.Date,
                                    ed,
                                    tl,
                                    flag == -1 ? 1 : 0
                                ));
                            }
                            else
                            {
                                Entity cEtn = isBefore == true ? bls[0] : bls[pt1.index];
                                DateTime cDate = cEtn == null ? new DateTime() : (((DateTime)cEtn["new_ngayapdung"])).AddHours(7);

                                if (isBefore == true)
                                {
                                    for (int i = pt1.index; i < pt2.index + 1; i++)
                                    {
                                        Entity etmp = null;
                                        DateTime tmp = new DateTime();

                                        if (i == -1)
                                        {
                                            tmp = begin;
                                        }
                                        else
                                        {
                                            etmp = bls[i];
                                            tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);
                                        }

                                        if (i == pt1.index)
                                        {
                                            if (i == -1)
                                            {
                                                i++;
                                                Entity etmp1 = bls[i];
                                                DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                                DateTime bg = begin;

                                                int sn = (int)CompareDate(tmp1, bg);
                                                decimal pLs = pbdt.laisuat;
                                                //pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    bg,
                                                    tmp1.Date,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp1;
                                                cEtn = etmp1;
                                            }
                                            else
                                            {
                                                #region
                                                i++;
                                                Entity etmp1 = bls[i];
                                                DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                                DateTime bg = tmp.Date.AddDays(pt1.sn);

                                                int sn = (int)CompareDate(tmp1, bg);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                //pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    bg,
                                                    tmp1.Date,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp1;
                                                cEtn = etmp1;
                                                #endregion
                                            }
                                        }
                                        else if (i != pt2.index)
                                        {
                                            //ls.Add((int)tmp.Date.Subtract(cDate.Date).TotalDays);
                                            int sn = (int)CompareDate(tmp, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            //pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls[pbdt.ID].Add(new phieutinhlai
                                            (
                                                sn,
                                                sotien,
                                                pLs,
                                                cDate.Date,
                                                tmp.Date,
                                                tl,
                                                flag == -1 ? 1 : 0
                                            ));
                                            cDate = tmp;
                                            cEtn = etmp;
                                        }
                                        else if (i == pt2.index)
                                        {
                                            if (pt2.isOver == true)
                                            {
                                                int snTmpI = (int)tmp.Date.Subtract(cDate.Date).TotalDays;
                                                decimal pLsTmpI = (decimal)cEtn["new_phantramlaisuat"];
                                                //pLsTmpI = pLsTmpI > laibandau ? laibandau : pLsTmpI;
                                                decimal tlTmpI = Math.Round((decimal)(snTmpI * pLsTmpI * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tlTmpI;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    snTmpI,
                                                    sotien,
                                                    pLsTmpI,
                                                    cDate,
                                                    tmp.Date,
                                                    tlTmpI,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp;
                                                cEtn = etmp;

                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            else
                                            {
                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                //pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    for (int i = pt1.index; i < pt2.index + 1; i++)
                                    {
                                        Entity etmp = bls[i];
                                        DateTime tmp = (((DateTime)etmp["new_ngayapdung"])).AddHours(7);

                                        if (i == pt1.index)
                                        {
                                            i++;
                                            Entity etmp1 = bls[i];
                                            DateTime tmp1 = (((DateTime)etmp1["new_ngayapdung"])).AddHours(7);
                                            DateTime bg = tmp.Date.AddDays(pt1.sn);

                                            int sn = (int)CompareDate(tmp1, bg);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            //pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);

                                            result += tl;
                                            ls[pbdt.ID].Add(new phieutinhlai
                                            (
                                                sn,
                                                sotien,
                                                pLs,
                                                bg,
                                                tmp1.Date,
                                                tl,
                                                flag == -1 ? 1 : 0
                                            ));
                                            cDate = tmp1;
                                            cEtn = etmp1;

                                        }
                                        else if (i != pt2.index)
                                        {
                                            //ls.Add((int)tmp.Date.Subtract(cDate.Date).TotalDays);
                                            int sn = (int)CompareDate(tmp, cDate);
                                            decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                            //pLs = pLs > laibandau ? laibandau : pLs;
                                            decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                            result += tl;
                                            ls[pbdt.ID].Add(new phieutinhlai
                                            (
                                                sn,
                                                sotien,
                                                pLs,
                                                cDate.Date,
                                                tmp.Date,
                                                tl,
                                                flag == -1 ? 1 : 0
                                            ));
                                            cDate = tmp;
                                            cEtn = etmp;
                                        }
                                        else if (i == pt2.index)
                                        {
                                            if (pt2.isOver == true)
                                            {
                                                int snTmpI = (int)tmp.Date.Subtract(cDate.Date).TotalDays;
                                                decimal pLsTmpI = (decimal)cEtn["new_phantramlaisuat"];
                                                //pLsTmpI = pLsTmpI > laibandau ? laibandau : pLsTmpI;
                                                decimal tlTmpI = Math.Round((decimal)(snTmpI * pLsTmpI * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tlTmpI;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    snTmpI,
                                                    sotien,
                                                    pLsTmpI,
                                                    cDate,
                                                    tmp.Date,
                                                    tlTmpI,
                                                    flag == -1 ? 1 : 0
                                                ));
                                                cDate = tmp;
                                                cEtn = etmp;

                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                //pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            else
                                            {
                                                DateTime ed = tmp.Date.AddDays(pt2.sn);
                                                int sn = (int)CompareDate(ed, cDate);
                                                decimal pLs = (decimal)cEtn["new_phantramlaisuat"];
                                                //pLs = pLs > laibandau ? laibandau : pLs;
                                                decimal tl = Math.Round((decimal)(sn * pLs * sotien / 3000), MidpointRounding.AwayFromZero);
                                                result += tl;
                                                ls[pbdt.ID].Add(new phieutinhlai
                                                (
                                                    sn = sn,
                                                    sotien,
                                                    pLs,
                                                    cDate.Date,
                                                    ed,
                                                    tl,
                                                    flag == -1 ? 1 : 0
                                                ));
                                            }
                                            break;
                                        }
                                    }
                                }
                            }

                            if (pbdt.status == 1 && flag != -1)
                            {
                                #region Create PTL
                                Guid id = Guid.Empty;
                                Createphieutinhlai(pbdt, sotien, result, ref id);
                                pbdt.status = 0;
                                var sorted = ls[pbdt.ID].OrderBy(p => p.to).ToList();

                                foreach (phieutinhlai o in sorted)
                                {
                                    if (o.status == 0)
                                        Createdoantinhlai(id, o.fr, o.to, o.ls, o.tl, o.sotien, o.sn);
                                }
                                #endregion
                            }
                            return result;
                        }

                        #endregion

                    }
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

        private Phanbodautu CreatePBDT(Guid enID) // en : pbdt
        {
            Entity en = service.Retrieve("new_phanbodautu", enID, new ColumnSet(true));
            Phanbodautu pbdt = new Phanbodautu();
            pbdt.ID = en.Id;
            pbdt.sotien = en.Contains("new_conlai") ? ((Money)en["new_conlai"]).Value : new decimal(0);

            if (!en.Contains("new_ngayphatsinh"))
                throw new Exception(en["new_name"].ToString() + " không có ngày phát sinh");

            if (!en.Contains("new_vuthanhtoan"))
                throw new Exception(en["new_name"].ToString() + " không có vụ thanh toán");

            if (!en.Contains("new_vudautu"))
                throw new Exception(en["new_name"].ToString() + " không có vụ đầu tư");

            Entity vuthanhtoan = service.Retrieve("new_vudautu", ((EntityReference)en["new_vuthanhtoan"]).Id,
                new ColumnSet(new string[] { "new_namtaichinhh", "new_name" }));

            if (!vuthanhtoan.Contains("new_namtaichinhh"))
                throw new Exception(vuthanhtoan["new_name"].ToString() + " không có năm tài chính");

            if (!en.Contains("new_loaihopdong"))
                throw new Exception(en["new_name"].ToString() + " không có loại hợp đồng");

            pbdt.loaihopdong = ((OptionSetValue)en["new_loaihopdong"]).Value;
            pbdt.masohphieuphanbo = en["new_maphieuphanbo"].ToString();
            pbdt.namtaichinhvuthanhtoan = Int32.Parse((string)vuthanhtoan["new_namtaichinhh"]);
            pbdt.ngaytinhlaisaucung = en.Contains("new_ngaytinhlaisaucung") ? (DateTime)en["new_ngaytinhlaisaucung"] : (DateTime)en["new_ngayphatsinh"];
            pbdt.ngayphatsinh = (DateTime)en["new_ngayphatsinh"];
            pbdt.nolai = en.Contains("new_nolai") ? ((Money)en["new_nolai"]).Value : new decimal(0);
            pbdt.vudautu = (EntityReference)en["new_vudautu"];
            pbdt.loailaisuat = en.Contains("new_loailaisuat") ? ((OptionSetValue)en["new_loailaisuat"]).Value : 0;
            pbdt.status = 1;

            if (!en.Contains("new_laisuat"))
                throw new Exception(en["new_name"].ToString() + " thiếu lãi suất cố định");

            pbdt.laisuat = (decimal)en["new_laisuat"];


            return pbdt;
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

        private void Createphieutinhlai(Phanbodautu pbdt, decimal tiengoc, decimal tl, ref Guid idPhieutinhlai)
        {
            EntityReference bbdccn = new EntityReference("new_bienbandoichieucongno", enId);
            Entity Upbdt = service.Retrieve("new_phanbodautu", pbdt.ID, new ColumnSet(new string[] { "new_ngayphatsinh" }));
            Entity phieutinhlai = new Entity("new_phieutinhlai");
            phieutinhlai["new_ngayvay"] = Upbdt["new_ngayphatsinh"];
            phieutinhlai["new_ngaytra"] = ngaythu;
            phieutinhlai["new_bienbandoichieucongno"] = bbdccn;
            phieutinhlai["new_phanbodautu"] = Upbdt.ToEntityReference();
            phieutinhlai["new_trgc"] = new Money(tiengoc);
            phieutinhlai["new_tienlai"] = new Money(tl);
            idPhieutinhlai = service.Create(phieutinhlai);
        }

        private void Createdoantinhlai(Guid idPhieutinhlai, DateTime ngayvay, DateTime ngaytra,
            decimal laisuat, decimal tienlai, decimal datragoc, int songay)
        {
            Entity doantinhlai = new Entity("new_doantinhlai");
            doantinhlai["new_name"] = ngayvay.Date.ToString("dd/MM/yyyy") + "-" + ngaytra.Date.ToString("dd/MM/yyyy");
            doantinhlai["new_phieutinhlai"] = new EntityReference("new_phieutinhlai", idPhieutinhlai);
            doantinhlai["new_tungay"] = ngayvay;
            doantinhlai["new_denngay"] = ngaytra;
            doantinhlai["new_laisuat"] = laisuat;
            doantinhlai["new_songay"] = songay;
            doantinhlai["new_tienlai"] = new Money(tienlai);
            doantinhlai["new_tiengoc"] = new Money(datragoc);

            Guid pID = service.Create(doantinhlai);
        }
    }
}
