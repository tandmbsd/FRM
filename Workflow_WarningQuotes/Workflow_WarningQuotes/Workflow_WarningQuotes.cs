using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Workflow;
using Microsoft.Xrm.Sdk.Query;
using System.Activities;
using Microsoft.Crm.Sdk.Messages;

namespace Workflow_WarningQuotes
{
    public class Workflow_WarningQuotes : CodeActivity
    {
        [RequiredArgument]
        [Input("InputEntity")]
        [ReferenceTarget("quote")]

        public InArgument<EntityReference> inputEntity { get; set; }
        public IOrganizationService service;
        public ITracingService tracingService;

        protected override void Execute(CodeActivityContext executionContext)
        {
            int pageNumber = 1;
            int fetchCount = 5000;

            tracingService = executionContext.GetExtension<ITracingService>();
            IWorkflowContext context = executionContext.GetExtension<IWorkflowContext>();
            IOrganizationServiceFactory serviceFactory = executionContext.GetExtension<IOrganizationServiceFactory>();
            service = serviceFactory.CreateOrganizationService(context.UserId);

            Guid entityId = context.PrimaryEntityId;

            Entity target = service.Retrieve(context.PrimaryEntityName, entityId, new ColumnSet(true));

            //Entity quote = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

            QueryExpression q = new QueryExpression("quote");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 1));
            q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 2));

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
                        process(b);
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
        //header_effectiveto_c
        void process(Entity quote)
        {            
            if (!quote.Contains("effectiveto"))
                return;
            
            DateTime ef = (DateTime)quote["effectiveto"];

            if (ef < DateTime.Now)
            {                
                CloseQuoteRequest req = new CloseQuoteRequest();
                Entity quoteClose = new Entity("quoteclose");
                quoteClose.Attributes.Add("quoteid", new EntityReference("quote", quote.Id));
                quoteClose.Attributes.Add("subject", "Customer was mean so we just closed it.");
                req.QuoteClose = quoteClose;
                req.RequestName = "CloseQuote";
                OptionSetValue o = new OptionSetValue();
                o.Value = 5;
                req.Status = o;
                CloseQuoteResponse resp = (CloseQuoteResponse)service.Execute(req);
            }
        }
    }
}
