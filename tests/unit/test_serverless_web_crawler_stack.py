import aws_cdk as core
import aws_cdk.assertions as assertions

from serverless_web_crawler.serverless_web_crawler_stack import ServerlessWebCrawlerStack

# example tests. To run these tests, uncomment this file along with the example
# resource in serverless_web_crawler/serverless_web_crawler_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ServerlessWebCrawlerStack(app, "serverless-web-crawler")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
