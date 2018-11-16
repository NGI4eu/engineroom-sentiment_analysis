# EngineRoom: Sentiment Analysis
Sentiment analysis applied to some words of the Reddit data set.

The English LIWC dictionary is needed to run a part of this Jupyter. For seeing and playing with the last Visualization, is not needed.

Methods available to run without the English LIWC dictionary:

 - plot_multiple_sentiment_analysis(articles): Visualization of the values of the sentiment analysis of the given articles.
 
     - Input: list of articles.
              
     - Output: Visualization of the values of the sentiment analysis of the given articles.
              
     ![alt text](https://github.com/NGI4eu/engineroom-sentiment_analysis/blob/master/images/visualization_sentiments.png)


- plot_ranking(sentiment, top_x=5, max_values=True): Plot the top X articles of a given Sentiment.

     - Input:
     
             - sentiment: the sentiment that you want to analyze. Choose one of the shown in the variable 'keys' (keys = 'past', 'present', 'future', 'swear', 'affect', 'posemo', 'negemo', 'anx', 'anger', 'sad', 'discrep').
             - max_values: max_values=True if you want to see the articles with the top values in that sentiment, or write max_values=False to find the articles with the minimum value.
             - top_x: the number of articles that you want to plot. top_x=5, plot the top 5 articles.
        
     - Output: Plot the top X articles of a given Sentiment
