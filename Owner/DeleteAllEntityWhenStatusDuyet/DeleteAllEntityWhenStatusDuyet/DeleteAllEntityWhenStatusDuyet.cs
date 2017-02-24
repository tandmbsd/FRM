using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace DeleteAllEntityWhenStatusDuyet
{
    public class DeleteAllEntityWhenStatusDuyet : IPlugin
    {
        private IOrganizationServiceFactory factory;
        private IOrganizationService service;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            var traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.Depth > 1)
                return;

            EntityReference target = (EntityReference)context.InputParameters["Target"];
            Entity fullEntity = (Entity)context.PreEntityImages["PreImg"];

            if (target.LogicalName == "new_phieudangkydichvu" || target.LogicalName == "new_phieudangkyhomgiong"
                || target.LogicalName == "new_phieudangkyphanbon" || target.LogicalName == "new_phieudangkythuoc"
                || target.LogicalName == "new_phieudangkyvattukhac")
            {
                if (fullEntity.Contains("new_tinhtrangduyet") && ((OptionSetValue)fullEntity["new_tinhtrangduyet"]).Value == 100000002)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_phieugiaonhanhomgiong")
            {
                if (fullEntity.Contains("new_tinhtrangduyet") && ((OptionSetValue)fullEntity["new_tinhtrangduyet"]).Value == 100000006)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_phieugiaonhanphanbon" || target.LogicalName == "new_phieugiaonhanthuoc"
                || target.LogicalName == "new_phieugiaonhanvattu")
            {
                if (fullEntity.Contains("new_tinhtrangduyet") && ((OptionSetValue)fullEntity["new_tinhtrangduyet"]).Value == 100000005)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietgiaonhanphanbon")
            {
                Entity pgnpb = service.Retrieve("new_phieugiaonhanphanbon",
                    ((EntityReference)fullEntity["new_phieugiaonhanphanbon"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pgnpb.Contains("new_tinhtrangduyet") && ((OptionSetValue)pgnpb["new_tinhtrangduyet"]).Value == 100000005)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietgiaonhanhomgiong")
            {
                Entity pgnhg = service.Retrieve("new_phieugiaonhanhomgiong",
                    ((EntityReference)fullEntity["new_phieugiaonhanhomgiong"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pgnhg.Contains("new_tinhtrangduyet") && ((OptionSetValue)pgnhg["new_tinhtrangduyet"]).Value == 100000006)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietgiaonhanthuoc")
            {
                Entity pgnt = service.Retrieve("new_phieugiaonhanthuoc",
                    ((EntityReference)fullEntity["new_phieugiaonhanthuoc"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pgnt.Contains("new_tinhtrangduyet") && ((OptionSetValue)pgnt["new_tinhtrangduyet"]).Value == 100000005)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietgiaonhanvattu")
            {
                Entity pgnvattu = service.Retrieve("new_phieugiaonhanvattu",
                    ((EntityReference)fullEntity["new_phieugiaonhanvattu"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pgnvattu.Contains("new_tinhtrangduyet") && ((OptionSetValue)pgnvattu["new_tinhtrangduyet"]).Value == 100000005)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietgiaonhanvattu")
            {
                Entity pgnvattu = service.Retrieve("new_phieugiaonhanvattu",
                    ((EntityReference)fullEntity["new_phieugiaonhanvattu"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pgnvattu.Contains("new_tinhtrangduyet") && ((OptionSetValue)pgnvattu["new_tinhtrangduyet"]).Value == 100000005)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietdangkydichvu")
            {
                Entity pdkdichvu = service.Retrieve("new_phieudangkydichvu",
                    ((EntityReference)fullEntity["new_phieudangkydichvu"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pdkdichvu.Contains("new_tinhtrangduyet") && ((OptionSetValue)pdkdichvu["new_tinhtrangduyet"]).Value == 100000002)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietdangkyhomgiong")
            {
                Entity pdkhomgiong = service.Retrieve("new_phieudangkyhomgiong",
                    ((EntityReference)fullEntity["new_phieudangkyhomgiong"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pdkhomgiong.Contains("new_tinhtrangduyet") && ((OptionSetValue)pdkhomgiong["new_tinhtrangduyet"]).Value == 100000002)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietdangkyphanbon")
            {
                Entity pdkphanbon = service.Retrieve("new_phieudangkyphanbon",
                    ((EntityReference)fullEntity["new_phieudangkyphanbon"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pdkphanbon.Contains("new_tinhtrangduyet") && ((OptionSetValue)pdkphanbon["new_tinhtrangduyet"]).Value == 100000002)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietdangkythuoc")
            {
                Entity pdkphanbon = service.Retrieve("new_phieudangkyphanbon",
                    ((EntityReference)fullEntity["new_phieudangkyphanbon"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pdkphanbon.Contains("new_tinhtrangduyet") && ((OptionSetValue)pdkphanbon["new_tinhtrangduyet"]).Value == 100000002)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietdangkyvattu")
            {
                Entity pdkphanbon = service.Retrieve("new_phieudangkyvattukhac",
                    ((EntityReference)fullEntity["new_phieudangkyvattukhac"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pdkphanbon.Contains("new_tinhtrangduyet") && ((OptionSetValue)pdkphanbon["new_tinhtrangduyet"]).Value == 100000002)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_chitietphieudenghithanhtoan")
            {
                Entity pdnthanhtoan = service.Retrieve("new_phieudenghithanhtoan",
                    ((EntityReference)fullEntity["new_phieudenghithanhtoan"]).Id,
                    new ColumnSet(new string[] { "new_tinhtrangduyet" }));

                if (pdnthanhtoan.Contains("new_tinhtrangduyet") && ((OptionSetValue)pdnthanhtoan["new_tinhtrangduyet"]).Value == 100000006)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
            else if (target.LogicalName == "new_phieudenghithanhtoan")
            {
                if (fullEntity.Contains("new_tinhtrangduyet") && ((OptionSetValue)fullEntity["new_tinhtrangduyet"]).Value == 100000006)
                {
                    throw new Exception("Phiếu đã duyệt không được phép xóa !!!");
                }
            }
        }
    }
}
