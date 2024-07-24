import boto3
import json
from urllib.parse import unquote_plus

# クライアントの初期化
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
table = dynamodb.Table('SweetsRecipes')

# カスタムモデルのARN
custom_model_arn = 'arn:aws:rekognition:ap-northeast-1:471112904122:project/sweets-rekognition/version/sweets-rekognition.2024-07-15T19.29.42/1721039382932'

def lambda_handler(event, context):
    try:
        for sqs_record in event['Records']:
            # SQSメッセージのbodyをJSON形式で読み込み
            message_body = json.loads(sqs_record['body'])
            for record in message_body['Records']:
                if 's3' in record:
                    # S3のバケット名とキーを取得
                    bucket = record['s3']['bucket']['name']
                    key = unquote_plus(record['s3']['object']['key'])

                    # Rekognitionを使って画像のカスタムラベル分析を実行
                    rekognition_response = rekognition.detect_custom_labels(
                        ProjectVersionArn=custom_model_arn,
                        Image={'S3Object': {'Bucket': bucket, 'Name': key}},
                        MaxResults=10
                    )

                    # Rekognitionからのレスポンスを処理
                    for label in rekognition_response['CustomLabels']:
                        sweet_name = label['Name']
                        # DynamoDBからレシピを照会
                        response = table.get_item(Key={'SweetName': 'カヌレ'})
                        # response = table.get_item(Key={'SweetName': sweet_name})
                        # response = table.get_item(
                        #     Key={
                        #         'SweetName': {
                        #             'S': sweet_name  # クエリする値も同じ形式にする
                        #         }
                        #     }
                        # )

                        # レスポンスから安全にデータを取得
                        item = response.get('Item')
                        if item and 'Recipe' in item:
                            recipe = item['Recipe']
                            print(f"スイーツ名「{sweet_name}」のレシピ: {recipe}")
                        else:
                            print(f"スイーツ名「{sweet_name}」のレシピは見つかりませんでした")

    except Exception as e:
        print(f"処理中に未処理の例外が発生しました: {e}")
        raise e  # ランタイムに例外を通知するために再スロー

    return {
        'statusCode': 200,
        'body': '画像の処理が正常に完了しました'
    }
