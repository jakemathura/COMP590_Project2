results = readtable('CancelsPerFlight.xlsx');
hold on
cdfplot(results.American_Airlines_Inc);
cdfplot(results.Alaska_Airlines_Inc);
cdfplot(results.JetBlue_Airways);
cdfplot(results.Delta_Airlines_Inc);
cdfplot(results.Atlantic_Southeast_Airlines);
cdfplot(results.Hawaiian_Airlines_Inc)
cdfplot(results.SkyWest_Airlines_Inc);
cdfplot(results.United_Airlines_Inc);
cdfplot(results.Southwest_Airlines_Co);
labels = {'American Airlines','Alaska Airlines','JetBlue Airways','Delta Airlines','Atlantic Southeast Airlines','Hawaiian Airlines','SkyWest Airlines','United Airlines','Southwest Airlines'};
legend(labels,'Location','bestoutside')