# EngineRoom: Sentiment Analysis
Sentiment analysis applied to some words of the Reddit dataset.

The English LIWC dictionary is needed to run a part of this Jupyter. For seeing and playing with the last Visualization, is not needed.

There is a notebook available (Sentiment_Analysis_LIWC.ipynb) with the methods to plot de Visualizations. We show an example below of the available Visualizations.


```
plot_multibar_sentiment(['gdpr', 'fake news','intellectual property' ])
```

![alt text](https://github.com/NGI4eu/engineroom-sentiment_analysis/blob/master/images/visualization_sentiments.png)

The X-axis shows the different categories of sentiments (past, present, future, swear, affect, positive emotions, negative emotions, anxiety, anger, sadness and discrepancy), and the Y-axis shows the percentage of words that fall in that category. 


```
plot_monthly_multiple_sentiments(['deletefacebook', 'brain-computer interface', 'autonomous weapon', 'competition law', 'hate speech', 'killer robot', 'metoo', 'digital surveillance', 'altright', 'freedom of speech'])
```
![alt text](https://github.com/NGI4eu/engineroom-sentiment_analysis/blob/master/images/reddit_monthly_multiple_sentiments.png)


