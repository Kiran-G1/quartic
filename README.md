# quartic

Hi this repo contains 3 scala objects

1)  quartic.scala=> which contains code used to develop the model.
                                  XGBoost has poor performance because XGBoost can't handle data with so many missing values.
                                  Spark improvised GBT was used for this data which indeed gave better results.
                                  You can find the observations in observations.pdf sent to your mail
                                 
 2)QuarticTest.scala=> This a streaming code which we used to load the trained model
                                          Code is by default taking data from port 9999 in local mode.
                                          
3)featurePusher=>      Since I don't have kafka installed, I've created a socket program which pushes data to port-9999

Also you can find 3 jar packages which I have downloaded to use XGBoost since Spark doesn't have integration with XGBoost.


