using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using System.ServiceModel;
using Microsoft.Xrm.Sdk.Workflow;
using System.Activities;

namespace Workflow_CapNhatTheXeDuyetHDVanChuyen
{
    public sealed partial class Workflow_CapNhatTheXeDuyetHDVanChuyen : CodeActivity
    {
        [RequiredArgument]
        [Input("HĐ vận chuyển")]
        [ReferenceTarget("new_hopdongvanchuyen")]
        public InArgument<EntityReference> tmp { get; set; }

        protected override void Execute(CodeActivityContext executionContext)
        {
            EntityReference target = this.tmp.Get(executionContext);
            IWorkflowContext context = executionContext.GetExtension<IWorkflowContext>();
            IOrganizationServiceFactory serviceFactory = executionContext.GetExtension<IOrganizationServiceFactory>();
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

            Entity HDVC = service.Retrieve("new_hopdongvanchuyen", target.Id, new ColumnSet("statuscode"));

            if (((OptionSetValue)HDVC["statuscode"]).Value == 100000000)
            {

                QueryExpression qe = new QueryExpression("new_hopdongvanchuyen_xevanchuyen");
                qe.ColumnSet = new ColumnSet("new_hopdongvanchuyen_xevanchuyenid");
                qe.Criteria.Conditions.Add(new ConditionExpression("new_hopdongvanchuyen", ConditionOperator.Equal, target.Id));
                qe.Criteria.Conditions.Add(new ConditionExpression("statuscode", ConditionOperator.Equal, 1));

                ExecuteMultipleRequest meq = new ExecuteMultipleRequest
                {
                    Settings = new ExecuteMultipleSettings()
                    {
                        ContinueOnError = true,
                        ReturnResponses = true
                    },
                    Requests = new OrganizationRequestCollection()
                };

                foreach (Entity a in service.RetrieveMultiple(qe).Entities)
                {
                    a["statuscode"] = new OptionSetValue(100000001);
                    UpdateRequest rq = new UpdateRequest { Target = a };
                    meq.Requests.Add(rq);
                }

                if (meq.Requests.Count > 0)
                {
                    ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)service.Execute(meq);
                    StringBuilder err = new StringBuilder();
                    foreach (var responseItem in responseWithResults.Responses)
                    {
                        if (responseItem.Fault != null)
                            err.AppendLine("Thẻ xe: " + ((UpdateRequest)meq.Requests[responseItem.RequestIndex]).Target.Id.ToString() + " - lỗi: " + responseItem.Fault.Message);
                    }
                    if (err.ToString() != string.Empty)
                        throw new Exception("Có lỗi khi update trạng thái thẻ xe: \r\n" + err.ToString());
                }
            }
        }
    }
}
