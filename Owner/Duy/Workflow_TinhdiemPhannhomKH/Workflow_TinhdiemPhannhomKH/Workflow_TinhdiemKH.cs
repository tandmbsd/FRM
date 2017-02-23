using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Workflow;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;
using System.Activities;

namespace Workflow_TinhdiemKH
{
    public class Workflow_TinhdiemKH : CodeActivity
    {
        [RequiredArgument]
        [Input("InputEntity")]
        [ReferenceTarget("new_vudautu")]
        public InArgument<EntityReference> inputEntity { get; set; }

        public IOrganizationService service;
        public ITracingService tracingService;
        protected override void Execute(CodeActivityContext executionContext)
        {
            tracingService = executionContext.GetExtension<ITracingService>();
            IWorkflowContext context = executionContext.GetExtension<IWorkflowContext>();
            IOrganizationServiceFactory serviceFactory = executionContext.GetExtension<IOrganizationServiceFactory>();
            service = serviceFactory.CreateOrganizationService(context.UserId);

            Guid enId = this.inputEntity.Get(executionContext).Id;
            EntityReference entrf = this.inputEntity.Get(executionContext);
            #region khach hang

            RetrieveALLKhachhang(entrf);

            #endregion

        }
        Entity FindVudautu()
        {
            Entity CurrVudautu = RetrieveSingleRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true);
            //Entity CurrVudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(true));
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection entc = service.RetrieveMultiple(q);

            List<Entity> lst = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == CurrVudautu.Id);

            return lst[curr - 3];

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
        Entity RetrieveSingleRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>().FirstOrDefault();
        }

        bool tinhdiem(int pheptinhtu, decimal giatritu, int pheptinhden, decimal giatriden, decimal value)
        {
            bool Fgiatritu = false;
            bool Fgiatriden = false;
            bool ketqua = false;
            switch (pheptinhtu)
            {
                case 100000000: //=
                    if (value == giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000001: // < 
                    if (value < giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000002: // >
                    if (value > giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000003: // <=
                    if (value <= giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                case 100000004: // >=
                    if (value >= giatritu)
                    {
                        Fgiatritu = true;
                    }
                    break;
                default:
                    if (pheptinhtu == -1)
                    {
                        Fgiatritu = true;
                    }
                    break;
            }
            switch (pheptinhden)
            {
                case 100000000: //=
                    if (value == giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000001: // < 
                    if (value < giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000002: // >
                    if (value > giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000003: // <=
                    if (value <= giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                case 100000004: // >=
                    if (value >= giatriden)
                    {
                        Fgiatriden = true;
                    }
                    break;
                default:
                    if (pheptinhden == -1)
                    {
                        Fgiatriden = true;
                    }
                    break;
            }

            if (Fgiatritu && Fgiatriden)
            {
                ketqua = true;
            }

            return ketqua;
        }
        decimal sumdientichthucte(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichconlai") ? en.GetAttributeValue<decimal>("new_dientichconlai") : 0;
            }

            return temp;
        }

        decimal sumdientichmiatuoi(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_dientichmiatuoi") ? en.GetAttributeValue<decimal>("new_dientichmiatuoi") : 0;
            }

            return temp;
        }
        decimal sumnangsuatbinhquan(EntityCollection thuacanhtac)
        {
            decimal temp = 0;
            foreach (Entity en in thuacanhtac.Entities)
            {
                temp += en.Contains("new_nangsuatbinhquan") ? en.GetAttributeValue<decimal>("new_nangsuatbinhquan") : 0;
            }

            return temp;
        }

        Entity RetrieveBac()
        {
            QueryExpression q = new QueryExpression("new_nhomkhachhang");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ma", ConditionOperator.Equal, "BAC"));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList().FirstOrDefault();

        }

        void RetrieveALLKhachhang(EntityReference target)
        {
            int pageNumber = 1;
            int fetchCount = 5000;
            int i = 1;

            QueryExpression q = new QueryExpression("contact");
            q.ColumnSet = new ColumnSet(new string[] { "new_thoigianhoptac", "new_nangsuatbinhquan", "new_culybinhquan", "new_nangluctaichinh_diem", "new_nangsuatduong", "new_makhachhang" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_loaikh", ConditionOperator.Like, "100000000" + "%"));

            q.PageInfo = new PagingInfo();
            q.PageInfo.Count = fetchCount;
            q.PageInfo.PageNumber = pageNumber;
            q.PageInfo.PagingCookie = null;

            while (true)
            {
                // Retrieve the page.
                EntityCollection insert = service.RetrieveMultiple(q);

                if (insert.Entities.Count > 0)
                {
                    foreach (Entity b in insert.Entities)
                    {
                        tracingService.Trace("Khách hàng thứ: " + i.ToString() + " -Pagenumber: " + q.PageInfo.PageNumber.ToString());
                        process(b, target);
                        i++;
                    }
                }

                // Check for more records, if it returns true.
                if (insert.MoreRecords)
                {
                    // Increment the page number to retrieve the next page.
                    q.PageInfo.PageNumber++;

                    // Set the paging cookie to the paging cookie returned from current results.
                    q.PageInfo.PagingCookie = insert.PagingCookie;
                }
                else
                    break;
            }
        }

        void process(Entity kh, EntityReference target)
        {
            #region process
            tracingService.Trace("start");
            Entity khachhangEn = this.service.Retrieve(kh.LogicalName, kh.Id, new ColumnSet(new string[] { "new_thoigianhoptac", "new_nangsuatbinhquan", "new_culybinhquan", "new_nangluctaichinh_diem", "new_nangsuatduong", "new_makhachhang", "fullname" }));
            string khachhangEnID = khachhangEn.Id.ToString();
            decimal tongdiem = new decimal();
            decimal diemcong = new decimal();
            decimal dientichtrongmia = new decimal();
            decimal nangsuatbinhquan = new decimal();
            decimal a1 = 0;
            decimal nsbq = 0;
            decimal clbqV = 0;
            decimal qmdtV = 0;
            decimal nqhV = 0;
            decimal nltcV = 0;
            decimal g = 0;
            decimal h = 0;
            decimal c = 0;
            decimal e = 0;
            decimal f = 0;
            decimal b = 0;
            decimal d = 0;
            decimal tytrongTGHT = 0;
            decimal tytrongNSBQD = 0;
            decimal tytrongCLBQ = 0;
            decimal tytrongQMDT = 0;
            decimal tytrongNQH = 0;
            decimal tytrongNLTC = 0;
            string name = khachhangEn["fullname"].ToString();
            bool isNoquahan = false;

            //Entity vudautuhientai = RetrieveSingleRecord(service, "new_vudautu", new ColumnSet(true), "new_danghoatdong", true);
            Entity tieuchiphannhomKH = RetrieveSingleRecord(service, "new_tieuchiphannhomkhachhang",
                new ColumnSet(new string[] { "new_tytrong_tght", "new_tytrong_nsbq", "new_tytrong_clbq",
                    "new_tytrong_qmdt", "new_tytrong_nqh", "new_tytrong_nltc", "new_name" }), "new_vudautu", target.Id);

            if (tieuchiphannhomKH == null || tieuchiphannhomKH.Id == Guid.Empty)
                throw new Exception("Không có tiêu chi phân nhóm khách hàng cho vụ hiện tại");

            // tinh diem tieu chi 
            QueryExpression qHddtmhientai = new QueryExpression("new_hopdongdautumia");
            qHddtmhientai.ColumnSet = new ColumnSet(true);
            qHddtmhientai.Criteria = new FilterExpression();
            qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, target.Id));
            qHddtmhientai.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
            qHddtmhientai.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000003));

            EntityCollection entcHddtm = service.RetrieveMultiple(qHddtmhientai);
            List<Entity> lstHDDTM = entcHddtm.Entities.ToList<Entity>();

            if (lstHDDTM.Count <= 0)
                return;

            if (tieuchiphannhomKH.Contains("new_tytrong_tght"))
                tytrongTGHT = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_tght");

            if (tieuchiphannhomKH.Contains("new_tytrong_nsbq"))
                tytrongNSBQD = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nsbq");

            if (tieuchiphannhomKH.Contains("new_tytrong_clbq"))
                tytrongCLBQ = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_clbq");

            if (tieuchiphannhomKH.Contains("new_tytrong_qmdt"))
                tytrongQMDT = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_qmdt");

            if (tieuchiphannhomKH.Contains("new_tytrong_nqh"))
                tytrongNQH = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nqh");

            if (tieuchiphannhomKH.Contains("new_tytrong_nltc"))
                tytrongNLTC = tieuchiphannhomKH.GetAttributeValue<decimal>("new_tytrong_nltc");

            tracingService.Trace("Thoi gian hop tac");
            #region Thoigianhoptac
            QueryExpression q = new QueryExpression("new_cosotinhdiem");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
            EntityCollection entc = service.RetrieveMultiple(q);
            List<Entity> lstCosotinhdiemTGHT = entc.Entities.ToList<Entity>();

            foreach (Entity TGHT in lstCosotinhdiemTGHT)
            {
                int pheptinhtu = TGHT.Contains("new_pheptinhtu") ? ((OptionSetValue)TGHT["new_pheptinhtu"]).Value : -1;
                int pheptinhden = TGHT.Contains("new_pheptinhden") ? ((OptionSetValue)TGHT["new_pheptinhden"]).Value : -1;

                if (!khachhangEn.Contains("new_thoigianhoptac"))
                    break;

                decimal tght = (decimal)(khachhangEn.GetAttributeValue<int>("new_thoigianhoptac"));

                if (tinhdiem(pheptinhtu, TGHT.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, TGHT.GetAttributeValue<decimal>("new_giatriden"), tght))
                {
                    a1 = ((decimal)(TGHT.GetAttributeValue<int>("new_diem")) * tytrongTGHT / 100);
                    tongdiem = tongdiem + a1;
                }
            }

            #endregion
            tracingService.Trace("Nang suat binh quan duong");
            #region NSBQD
            QueryExpression q1 = new QueryExpression("new_cosotinhdiem");
            q1.ColumnSet = new ColumnSet(true);
            q1.Criteria = new FilterExpression();
            q1.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, 100000001));
            q1.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
            EntityCollection entc1 = service.RetrieveMultiple(q1);
            List<Entity> lstCosotinhdiemNSBQD = entc1.Entities.ToList<Entity>();

            QueryExpression qVudautu = new QueryExpression("new_vudautu");
            qVudautu.ColumnSet = new ColumnSet(true);
            qVudautu.Criteria = new FilterExpression();
            qVudautu.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection a = service.RetrieveMultiple(qVudautu);

            List<Entity> lst = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == target.Id);

            List<Entity> lstHDDTM3nam = new List<Entity>();
            Entity khEditNSBQ = new Entity("contact");

            if (3 <= curr)
            {
                for (int i = 1; i <= 3; i++)
                {
                    QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                    qHddtm3nam.ColumnSet = new ColumnSet(true);
                    qHddtm3nam.Criteria = new FilterExpression();
                    qHddtm3nam.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - i].Id));
                    qHddtm3nam.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                    EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtm3nam);
                    lstHDDTM3nam = entcHddtm3nam.Entities.ToList<Entity>();
                    nangsuatbinhquan += sumnangsuatbinhquan(new EntityCollection(lstHDDTM3nam));
                }

            }
            else if (curr == 2)
            {
                for (int i = 1; i <= 2; i++)
                {
                    QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                    qHddtm3nam.ColumnSet = new ColumnSet(true);
                    qHddtm3nam.Criteria = new FilterExpression();
                    qHddtm3nam.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - i].Id));
                    qHddtm3nam.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                    EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtm3nam);
                    lstHDDTM3nam = entcHddtm3nam.Entities.ToList<Entity>();
                    nangsuatbinhquan += sumnangsuatbinhquan(new EntityCollection(lstHDDTM3nam));

                }
            }
            else if (curr == 1)
            {
                QueryExpression qHddtm3nam = new QueryExpression("new_hopdongdautumia");
                qHddtm3nam.ColumnSet = new ColumnSet(true);
                qHddtm3nam.Criteria = new FilterExpression();
                qHddtm3nam.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, lst[curr - 1].Id));
                qHddtm3nam.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                EntityCollection entcHddtm3nam = service.RetrieveMultiple(qHddtm3nam);
                Entity temp = entcHddtm3nam.Entities.ToList<Entity>().FirstOrDefault();

                nangsuatbinhquan = temp.Contains("new_nangsuatbinhquan") ? temp.GetAttributeValue<decimal>("new_nangsuatbinhquan") : 0;
            }

            khEditNSBQ = service.Retrieve(khachhangEn.LogicalName, khachhangEn.Id, new ColumnSet(new string[] { "new_nangsuatduong", "new_makhachhang" }));
            khEditNSBQ["new_nangsuatduong"] = nangsuatbinhquan;

            service.Update(khEditNSBQ);

            foreach (Entity NSBQD in lstCosotinhdiemNSBQD)
            {
                int pheptinhtu = NSBQD.Contains("new_pheptinhtu") ? ((OptionSetValue)NSBQD["new_pheptinhtu"]).Value : -1;
                int pheptinhden = NSBQD.Contains("new_pheptinhden") ? ((OptionSetValue)NSBQD["new_pheptinhden"]).Value : -1;

                if (!khEditNSBQ.Contains("new_nangsuatduong"))
                    break;

                //decimal nangsuatduong = (decimal)(khachhangEn.GetAttributeValue<decimal>("new_nangsuatduong"));

                if (tinhdiem(pheptinhtu, NSBQD.Contains("new_giatritu") ? NSBQD.GetAttributeValue<decimal>("new_giatritu") : -1, pheptinhden, NSBQD.Contains("new_giatriden") ? NSBQD.GetAttributeValue<decimal>("new_giatriden") : -1, nangsuatbinhquan))
                {
                    nsbq = (decimal)(NSBQD.GetAttributeValue<int>("new_diem")) * tytrongNSBQD / 100;
                    tongdiem = tongdiem + nsbq;
                }
            }

            #endregion
            tracingService.Trace("Cu li binh quan");
            #region Culibinhquan
            QueryExpression q2 = new QueryExpression("new_cosotinhdiem");
            q2.ColumnSet = new ColumnSet(true);
            q2.Criteria = new FilterExpression();
            q2.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, 100000002));
            q2.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
            EntityCollection entc2 = service.RetrieveMultiple(q2);
            List<Entity> lstCosotinhdiemCLBQ = entc2.Entities.ToList<Entity>();

            foreach (Entity CLBQ in lstCosotinhdiemCLBQ)
            {
                int pheptinhtu = CLBQ.Contains("new_pheptinhtu") ? ((OptionSetValue)CLBQ["new_pheptinhtu"]).Value : -1;
                int pheptinhden = CLBQ.Contains("new_pheptinhden") ? ((OptionSetValue)CLBQ["new_pheptinhden"]).Value : -1;

                if (!khachhangEn.Contains("new_culybinhquan"))
                    break;

                decimal clbq = khachhangEn.GetAttributeValue<decimal>("new_culybinhquan");

                if (tinhdiem(pheptinhtu, CLBQ.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, CLBQ.GetAttributeValue<decimal>("new_giatriden"), clbq))
                {
                    clbqV = (decimal)(CLBQ.GetAttributeValue<int>("new_diem")) * tytrongCLBQ / 100;
                    tongdiem = tongdiem + clbqV;
                }
            }

            #endregion
            tracingService.Trace("Qui mo dien tich");
            #region Quimodientich
            QueryExpression q3 = new QueryExpression("new_cosotinhdiem");
            q3.ColumnSet = new ColumnSet(true);
            q3.Criteria = new FilterExpression();
            q3.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, 100000003));
            q3.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
            EntityCollection entc3 = service.RetrieveMultiple(q3);
            List<Entity> lstCosotinhdiemQMDT = entc3.Entities.ToList<Entity>();
            decimal tongdientich = 0;

            foreach (Entity hopdongdautumia in lstHDDTM)
            {
                List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                    new ColumnSet(new string[] { "new_dientichconlai" }), "new_hopdongdautumia", hopdongdautumia.Id);
                tongdientich = tongdientich + sumdientichthucte(new EntityCollection(Lstthuadatcanhtac));
            }

            foreach (Entity QMDT in lstCosotinhdiemQMDT)
            {
                int pheptinhtu = QMDT.Contains("new_pheptinhtu") ? ((OptionSetValue)QMDT["new_pheptinhtu"]).Value : -1;
                int pheptinhden = QMDT.Contains("new_pheptinhden") ? ((OptionSetValue)QMDT["new_pheptinhden"]).Value : -1;

                if (tinhdiem(pheptinhtu, QMDT.GetAttributeValue<decimal>("new_giatritu"), pheptinhden, QMDT.GetAttributeValue<decimal>("new_giatriden"), tongdientich))
                {
                    qmdtV = (decimal)(QMDT.GetAttributeValue<int>("new_diem")) * tytrongQMDT / 100;
                    tongdiem = tongdiem + qmdtV;
                }
            }

            #endregion
            tracingService.Trace("no qua han");
            #region Noquahan
            QueryExpression q4 = new QueryExpression("new_cosotinhdiem");
            q4.ColumnSet = new ColumnSet(true);
            q4.Criteria = new FilterExpression();
            q4.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, 100000004));
            q4.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
            EntityCollection entc4 = service.RetrieveMultiple(q4);
            List<Entity> lstCosotinhdiemNQH = entc4.Entities.ToList<Entity>();
            decimal noquahan = 0;

            QueryExpression q5 = new QueryExpression("new_phanbodautu");
            q5.ColumnSet = new ColumnSet(true);
            q5.Criteria = new FilterExpression();
            q5.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, target.Id));
            q5.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
            EntityCollection entc5 = service.RetrieveMultiple(q5);
            List<Entity> lstPhanbodautu = entc5.Entities.ToList<Entity>();

            foreach (Entity pbdt in lstPhanbodautu)
            {
                if (!pbdt.Contains("new_vuthanhtoan"))
                    continue;

                Entity vuthanhtoan = service.Retrieve("new_vudautu", ((EntityReference)pbdt["new_vuthanhtoan"]).Id,
                    new ColumnSet(new string[] { "new_ngayketthuc" }));

                DateTime ngaythanhtoan = (DateTime)vuthanhtoan["new_ngayketthuc"];

                if (ngaythanhtoan < DateTime.Today)
                {
                    isNoquahan = true;
                    noquahan = noquahan + ((Money)pbdt["new_conlai"]).Value;
                }
            }

            foreach (Entity NQH in lstCosotinhdiemNQH)
            {
                int pheptinhtu = NQH.Contains("new_pheptinhtu") ? ((OptionSetValue)NQH["new_pheptinhtu"]).Value : -1;
                int pheptinhden = NQH.Contains("new_pheptinhden") ? ((OptionSetValue)NQH["new_pheptinhden"]).Value : -1;
                string cachtinh = ((OptionSetValue)NQH["new_cachtinh"]).Value.ToString();
                decimal giatritu = 0;
                decimal giatriden = 0;

                if (cachtinh == "100000001")
                {
                    giatritu = NQH.Contains("new_sotientu") ? ((Money)NQH["new_sotientu"]).Value : -1;
                    giatriden = NQH.Contains("new_sotienden") ? ((Money)NQH["new_sotienden"]).Value : -1;
                }
                else
                {
                    giatritu = NQH.Contains("new_giatritu") ? NQH.GetAttributeValue<decimal>("new_giatritu") : -1;
                    giatriden = NQH.Contains("new_giatriden") ? NQH.GetAttributeValue<decimal>("new_giatriden") : -1;
                }

                if (tinhdiem(pheptinhtu, giatritu, pheptinhden, giatriden, noquahan))
                {
                    nqhV = (decimal)(NQH.GetAttributeValue<int>("new_diem")) * tytrongNQH / 100;
                    tongdiem = tongdiem + nqhV;
                }
            }

            #endregion
            tracingService.Trace("Nang luc tai chinh");
            #region Nangluctaichinh
            QueryExpression q6 = new QueryExpression("new_cosotinhdiem");
            q6.ColumnSet = new ColumnSet(true);
            q6.Criteria = new FilterExpression();
            q6.Criteria.AddCondition(new ConditionExpression("new_loaitieuchi", ConditionOperator.Equal, "100000005"));
            q6.Criteria.AddCondition(new ConditionExpression("new_tieuchiphannhomkhachhang", ConditionOperator.Equal, tieuchiphannhomKH.Id));
            EntityCollection entc6 = service.RetrieveMultiple(q6);
            List<Entity> lstCosotinhdiemNLTC = entc6.Entities.ToList<Entity>();

            //if (!khachhangEn.Contains("new_nangluctaichinh_diem"))
            //    throw new Exception("Khách hàng chưa có năng lực tài chính");

            decimal nltc = khachhangEn.Contains("new_nangluctaichinh_diem") ? (decimal)(khachhangEn.GetAttributeValue<int>("new_nangluctaichinh_diem")) : 0;

            nltcV = (decimal)(nltc * tytrongNLTC / 100);
            tongdiem = tongdiem + nltcV;

            #endregion

            //throw new Exception(a1.ToString() + "-" + nsbq.ToString() + "-" + clbqV.ToString() + "-" + qmdtV.ToString() + "-" + nqhV.ToString() + "-" + nltcV.ToString());
            //tinh diem cong tru
            List<Entity> lstCachtinhdiemcongtru = RetrieveMultiRecord(service, "new_cachtinhdiemcongtru", new ColumnSet(true), "new_tieuchiphannhomkh", tieuchiphannhomKH.Id);
            foreach (Entity ctdiemcongtru in lstCachtinhdiemcongtru)
            {
                decimal tongdientichmiatuoi = 0;

                Entity hdonly = lstHDDTM.FirstOrDefault();
                decimal dientichtrongmiaonly = hdonly.GetAttributeValue<decimal>("new_dientichtrongmia");

                string loaitinhdiem = ((OptionSetValue)ctdiemcongtru["new_loaitinhdiem"]).Value.ToString();
                string phuongthuctinh = ((OptionSetValue)ctdiemcongtru["new_phuongthuctinh"]).Value.ToString();

                tracingService.Trace("tinh diem cong tru");
                if (loaitinhdiem == "100000000")//dien tich tuoi mia huu hieu > 30% dien tich hop dong 
                {
                    foreach (Entity hopdongdautumia in lstHDDTM)
                    {
                        if (hopdongdautumia.Contains("new_dientichtrongmia"))
                            dientichtrongmia += hopdongdautumia.GetAttributeValue<decimal>("new_dientichtrongmia");

                        List<Entity> Lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                            new ColumnSet(new string[] { "new_dientichmiatuoi" }), "new_hopdongdautumia", hopdongdautumia.Id);
                        tongdientichmiatuoi = tongdientichmiatuoi + sumdientichmiatuoi(new EntityCollection(Lstthuadatcanhtac));

                    }

                    if (phuongthuctinh == "100000000" && dientichtrongmia != 0) // tham so
                    {
                        int phuongthuctu = ctdiemcongtru.Contains("new_phuongthuctu") ? ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value : -1;
                        int phuongthucden = ctdiemcongtru.Contains("new_phuongthucden") ? ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value : -1;
                        decimal thamsotu = ctdiemcongtru.Contains("new_thamsotu") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu") : -1;
                        decimal thamsoden = ctdiemcongtru.Contains("new_thamsoden") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden") : -1;

                        decimal value = ((tongdientichmiatuoi / dientichtrongmia) * 100);

                        if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, value))
                        {
                            diemcong += ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }
                }
                else if (loaitinhdiem == "100000001") // dien tich dat nha chem ty le >= 50% tong dien tich hop dong 
                {
                    decimal dientichdatnha = 0;
                    List<Entity> lstthuacanhtacdatnha = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                            new ColumnSet(true), "new_hopdongdautumia", hdonly.Id);
                    foreach (Entity en in lstthuacanhtacdatnha)
                    {
                        Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)en["new_thuadat"]).Id, new ColumnSet(true));

                        string loaisohuudat = ((OptionSetValue)thuadat["new_loaisohuudat"]).Value.ToString();
                        if (loaisohuudat == "100000000")
                        {
                            dientichdatnha += en.GetAttributeValue<decimal>("new_dientichthucte");
                        }
                    }
                    if (phuongthuctinh == "100000000" && dientichtrongmia != 0) // tham so
                    {
                        int phuongthuctu = ctdiemcongtru.Contains("new_phuongthuctu") ? ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value : -1;
                        int phuongthucden = ctdiemcongtru.Contains("new_phuongthucden") ? ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value : -1;
                        decimal thamsotu = ctdiemcongtru.Contains("new_thamsotu") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu") : -1;
                        decimal thamsoden = ctdiemcongtru.Contains("new_thamsoden") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden") : -1;
                        decimal value = ((dientichdatnha / dientichtrongmia) * 100);

                        if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, value))
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            b = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        }
                    }

                }
                else if (loaitinhdiem == "100000002") // khach hang co dien dich >= 30ha
                {
                    if (phuongthuctinh == "100000000") // tham so
                    {
                        int phuongthuctu = ctdiemcongtru.Contains("new_phuongthuctu") ? ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value : -1;
                        int phuongthucden = ctdiemcongtru.Contains("new_phuongthucden") ? ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value : -1;
                        decimal thamsotu = ctdiemcongtru.Contains("new_thamsotu") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu") : -1;
                        decimal thamsoden = ctdiemcongtru.Contains("new_thamsoden") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden") : -1;

                        if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, dientichtrongmia))
                        {
                            diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                            c = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru"); ;
                        }
                    }

                }
                else if (loaitinhdiem == "100000003")//khach hang co sang kiến cong ty ghi nhận
                {
                    QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                    q.ColumnSet = new ColumnSet(true);
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                    q.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000000));
                    EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                    //List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                    if (enSangkienKH.Entities.Count > 0)
                    {
                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        d = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                    }

                }
                else if (loaitinhdiem == "100000004")//Có vi phạm hợp đồng và qui định của cty
                {
                    QueryExpression qBienbanvipham = new QueryExpression("new_bienbanvipham");
                    qBienbanvipham.ColumnSet = new ColumnSet(true);
                    qBienbanvipham.Criteria = new FilterExpression();
                    qBienbanvipham.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, target.Id));
                    qBienbanvipham.Criteria.AddCondition(new ConditionExpression("new_doitackh", ConditionOperator.Equal, khachhangEn.Id));
                    EntityCollection enBienbanvipham = service.RetrieveMultiple(qBienbanvipham);

                    //List<Entity> lstBienbanvipham = enBienbanvipham.Entities.ToList<Entity>();
                    if (enBienbanvipham.Entities.Count > 0)
                    {
                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        e = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                    }

                }
                else if (loaitinhdiem == "100000005")//Tỷ lệ sản lượng mía cháy /tổng sản lượng > 10% (của vụ liền kề)
                {
                    QueryExpression qNghiemthusauthuhoach = new QueryExpression("new_nghiemthuchatsatgoc");
                    qNghiemthusauthuhoach.ColumnSet = new ColumnSet(true);
                    qNghiemthusauthuhoach.Criteria = new FilterExpression();
                    qNghiemthusauthuhoach.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));

                    LinkEntity linkHopdongdautumia = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                    qNghiemthusauthuhoach.LinkEntities.Add(linkHopdongdautumia);

                    linkHopdongdautumia.LinkCriteria = new FilterExpression();
                    linkHopdongdautumia.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, target.Id));
                    EntityCollection enNghiemthusauthuhoach = service.RetrieveMultiple(qNghiemthusauthuhoach);

                    int phuongthuctu = ctdiemcongtru.Contains("new_phuongthuctu") ? ((OptionSetValue)ctdiemcongtru["new_phuongthuctu"]).Value : -1;
                    int phuongthucden = ctdiemcongtru.Contains("new_phuongthucden") ? ((OptionSetValue)ctdiemcongtru["new_phuongthucden"]).Value : -1;
                    decimal thamsotu = ctdiemcongtru.Contains("new_thamsotu") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsotu") : -1;
                    decimal thamsoden = ctdiemcongtru.Contains("new_thamsoden") ? ctdiemcongtru.GetAttributeValue<decimal>("new_thamsoden") : -1;

                    if (enNghiemthusauthuhoach.Entities.Count > 0)
                    {
                        Entity Nghiemthusauthuhoach = enNghiemthusauthuhoach.Entities.ToList<Entity>().FirstOrDefault();

                        List<Entity> lstchitietnghiemthu = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", Nghiemthusauthuhoach.Id);

                        foreach (Entity en in lstchitietnghiemthu)
                        {
                            decimal tongslmiachay = en.GetAttributeValue<decimal>("new_miachay");
                            decimal tongsl = en.GetAttributeValue<decimal>("new_tongsanluong");
                            if (tongsl != 0)
                            {
                                decimal phantram = (tongslmiachay / tongsl) * 100;

                                if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, phantram))
                                {
                                    diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                    f = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                }
                            }
                        }

                    }
                    else
                    {
                        QueryExpression qVudautu1 = new QueryExpression("new_vudautu");
                        qVudautu1.ColumnSet = new ColumnSet(true);
                        qVudautu1.Criteria = new FilterExpression();
                        qVudautu1.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                        EntityCollection en_qVudautu1 = service.RetrieveMultiple(qVudautu1);

                        List<Entity> lstVudautu = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                        int curr1 = lst.FindIndex(p => p.Id == target.Id);

                        Entity vudautulienke = service.Retrieve("new_vudautu", lst[curr1 - 1].Id, new ColumnSet(true));

                        QueryExpression qNghiemthusauthuhoachlienke = new QueryExpression("new_nghiemthuchatsatgoc");
                        qNghiemthusauthuhoachlienke.ColumnSet = new ColumnSet(true);
                        qNghiemthusauthuhoachlienke.Criteria = new FilterExpression();
                        qNghiemthusauthuhoachlienke.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));

                        LinkEntity linkHopdongdautumialienke = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                        qNghiemthusauthuhoachlienke.LinkEntities.Add(linkHopdongdautumialienke);

                        linkHopdongdautumialienke.LinkCriteria = new FilterExpression();
                        linkHopdongdautumialienke.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautulienke.Id));
                        EntityCollection enNghiemthusauthuhoachlienke = service.RetrieveMultiple(qNghiemthusauthuhoachlienke);

                        if (enNghiemthusauthuhoachlienke.Entities.Count > 0)
                        {
                            Entity Nghiemthusauthuhoach = enNghiemthusauthuhoachlienke.Entities.ToList<Entity>().FirstOrDefault();

                            List<Entity> lstchitietnghiemthu = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", Nghiemthusauthuhoach.Id);

                            foreach (Entity en in lstchitietnghiemthu)
                            {
                                decimal tongslmiachay = en.GetAttributeValue<decimal>("new_miachay");
                                decimal tongsl = en.GetAttributeValue<decimal>("new_tongsanluong");
                                if (tongsl != 0)
                                {
                                    decimal phantram = (tongslmiachay / tongsl) * 100;

                                    if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, phantram))
                                    {
                                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                        f = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                                    }
                                }
                            }
                        }
                    }

                }
                else if (loaitinhdiem == "100000006")//Khách hàng có ảnh hưởng đến các khách hàng khác trong công tác phát triển vùng nguyên liệu
                {
                    QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                    qSangkienKH.ColumnSet = new ColumnSet(true);
                    qSangkienKH.Criteria = new FilterExpression();
                    qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                    qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000001));
                    EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                    //List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                    if (enSangkienKH.Entities.Count > 0)
                    {
                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        g = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                    }
                }
                else if (loaitinhdiem == "100000007")//Khả năng phát triển DT qua từng năm DT có xu hướng tăng – không giảm
                {
                    QueryExpression qSangkienKH = new QueryExpression("new_ghinhandanhgiakhachhang");
                    qSangkienKH.ColumnSet = new ColumnSet(true);
                    qSangkienKH.Criteria = new FilterExpression();
                    qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, khachhangEn.Id));
                    qSangkienKH.Criteria.AddCondition(new ConditionExpression("new_loaighinhan", ConditionOperator.Equal, 100000002));
                    EntityCollection enSangkienKH = service.RetrieveMultiple(qSangkienKH);

                    //List<Entity> lstSangkienKH = enSangkienKH.Entities.ToList<Entity>();
                    if (enSangkienKH.Entities.Count > 0)
                    {
                        diemcong = diemcong + ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                        h = ctdiemcongtru.GetAttributeValue<decimal>("new_diemcongtru");
                    }
                }
                tracingService.Trace("ket thuc tinh diem cong tru");
            }

            tongdiem = tongdiem + diemcong;

            if (tongdiem > 10)
                tongdiem = 10;

            List<Entity> lstThangdiemphannhomKH = RetrieveMultiRecord(service, "new_thangdiemphannhomkhachhang", new ColumnSet(true), "new_vudautu", target.Id);

            Entity newKH = new Entity("contact");
            newKH = service.Retrieve(khachhangEn.LogicalName, khachhangEn.Id,
                new ColumnSet(new string[] { "new_nhomkhachhang", "new_diem" }));
            newKH["new_diem"] = tongdiem;

            int n = lstThangdiemphannhomKH.Count;

            for (int i = 0; i < n; i++)
            {
                int phuongthuctu = lstThangdiemphannhomKH[i].Contains("new_phuongthuctu") ? ((OptionSetValue)lstThangdiemphannhomKH[i]["new_phuongthuctu"]).Value : -1;
                int phuongthucden = lstThangdiemphannhomKH[i].Contains("new_phuongthucden") ? ((OptionSetValue)lstThangdiemphannhomKH[i]["new_phuongthucden"]).Value : -1;
                decimal thamsotu = lstThangdiemphannhomKH[i].Contains("new_diemtu") ? lstThangdiemphannhomKH[i].GetAttributeValue<decimal>("new_diemtu") : -1;
                decimal thamsoden = lstThangdiemphannhomKH[i].Contains("new_diemden") ? lstThangdiemphannhomKH[i].GetAttributeValue<decimal>("new_diemden") : -1;

                if (tinhdiem(phuongthuctu, thamsotu, phuongthucden, thamsoden, tongdiem))
                {
                    newKH["new_nhomkhachhang"] = lstThangdiemphannhomKH[i]["new_nhomkhachhang"];
                    bool isgoldorvip = false;

                    Entity nhomkhachhang = service.Retrieve("new_nhomkhachhang", ((EntityReference)lstThangdiemphannhomKH[i]["new_nhomkhachhang"]).Id
                        , new ColumnSet(new string[] { "new_ma" })); ;

                    if ((string)nhomkhachhang["new_ma"] == "VIP" || (string)nhomkhachhang["new_ma"] == "VANG")
                        isgoldorvip = true;

                    if (((OptionSetValue)lstThangdiemphannhomKH[i]["new_dieukien"]).Value == 100000001 && isgoldorvip == true) // ko có nợ quá hạn
                    {
                        if (isNoquahan == true)
                        {
                            newKH["new_nhomkhachhang"] = RetrieveBac().ToEntityReference();
                        }
                    }
                }
            }

            tracingService.Trace("tạo phiếu tính điểm");
            CreatePhieutinhdiemKH(name, a1, nsbq, clbqV, qmdtV, nqhV, nltcV, diemcong, target);
            service.Update(newKH);
            tracingService.Trace("end");
            //throw new Exception(a1.ToString() + "," + b.ToString() + "," + c.ToString() + "," + d.ToString() + "," + e.ToString() + "," + f.ToString() + "," + g.ToString() + "," + h.ToString() + "," + tongdiem.ToString() + "," + nangsuatbinhquan.ToString() + "," + noquahan1.ToString());
            #endregion
        }

        void CreatePhieutinhdiemKH(string name, decimal diemthoigianhoptac, decimal diemnangsuatbqduong, decimal diemculibinhquan,
            decimal diemquymodientich, decimal diemnoquahan, decimal diemnangluctaichinh, decimal diemcongtru, EntityReference vudautu)
        {
            Entity t = new Entity("new_phieutinhdiemkhachhang");

            t["new_name"] = name;
            t["new_vudautu"] = vudautu;
            t["new_diemthoigianhoptac"] = diemthoigianhoptac;
            t["new_diemnangsuatbinhquanduong"] = diemnangsuatbqduong;
            t["new_diemculybinhquan"] = diemculibinhquan;
            t["new_diemhuymodientich"] = diemquymodientich;
            t["new_diemnoquahan"] = diemnoquahan;
            t["new_diemnangluctaichinh"] = diemnangluctaichinh;
            t["new_diemcongtru"] = diemcongtru;

            service.Create(t);
        }
    }
}
