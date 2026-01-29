
## Cortex Analyst Name
JAL_RETAIL_ANALYTICS_SV

## Cortex Analyst Description
JAL の機内販売に関する分析用セマンティックビューです。
商品（PRODUCT）、顧客（CUSTOMER）、購入履歴（PURCHASE）、販売員 CA（STAFF）を対象に、"どの顧客がどの商品をいくら購入したか" を自然言語で聞けるようにすることを目的としています。

## Custom Instructions Sample

```
・売上の単位は「円」で表示してください
・日付フィルタがない場合は、直近1年分のデータを対象にしてください
・「CA」は「キャビンアテンダント」の略称です。STAFF テーブルを参照してください
・金額を表示するときは、カンマ区切りで見やすくしてください
```

**書くべき内容：**
- デフォルトの期間フィルタ（例：「指定がなければ直近1年」）
- 略語や社内用語の定義（例：「CA = キャビンアテンダント」）
- 出力フォーマットの指定（例：「金額はカンマ区切り」）
- ビジネス固有のルール（例：「売上は税込で計算」）


----
# AGENT

## Agent Name
AIRLINE_RETAIL_AGENT

## Agent Description
航空会社の機内販売データ分析エージェント


## Tool Name
query_retail_analytics

## Tool Description
機内販売データを分析するツール。購入履歴（PURCHASE）、商品マスター（PRODUCT）、顧客マスター（CUSTOMER）のデータを使って、売上分析、顧客分析、商品分析などの質問に回答できます。


## Orchestration
あなたは航空会社の機内販売データ分析エージェントです。日本語で回答してください。ユーザーの質問に対して、適切なツールを使用してデータを取得し、分析結果を分かりやすく説明してください。

## Custom Instractions
回答は日本語で、簡潔かつ分かりやすく提供してください。数値データがある場合は、適切にフォーマットして表示してください。

