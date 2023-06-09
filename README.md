<h1>K-Means Clustering with Hadoop MapReduce</h1>
<p>This project implements the K-Means clustering algorithm using Hadoop MapReduce. K-Means is a popular unsupervised machine learning algorithm used for clustering data points into K clusters.</p>
<h2>Overview</h2>
<p>The K-Means algorithm works by iteratively assigning data points to the nearest centroid and updating the centroids based on the
assigned points. The algorithm converges when the centroids no longer change significantly. In this project, we leverage the power of 
Hadoop MapReduce to distribute the computation and handle large-scale datasets.</p><h2>Usage</h2><p>To run the K-Means clustering 
with Hadoop MapReduce, follow the steps below:</p><ol><li><p>Ensure that you have Hadoop installed and configured properly on your system.</p>
</li><li><p>Compile the project using the provided Makefile or build script.</p></li><li>
<p>Prepare your input data. The input should be a text file with each line representing a data point with its coordinates. 
Each coordinate should be separated by a comma.</p></li><li><p>Run the following command to execute the K-Means algorithm:</p></li>
</ol><pre><div class="bg-black rounded-md mb-4"><div class="flex items-center relative text-gray-200 bg-gray-800 px-4 py-2 text-xs font-sans
justify-between rounded-t-md"><span>shell</span><button class="flex ml-auto gap-2"><svg stroke="currentColor" fill="none" stroke-width="2" viewBox="0 0 24 24" stroke-linecap="round" stroke-linejoin="round" class="h-4 w-4" height="1em" width="1em" xmlns="http://www.w3.org/2000/svg"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"></path><rect x="8" y="2" width="8" height="4" rx="1" ry="1"></rect></svg>Copy code</button></div><div class="p-4 overflow-y-auto"><code class="!whitespace-pre hljs language-shell">hadoop jar kmeans.jar org.example.Main &lt;input&gt; &lt;output&gt; &lt;k&gt;
</code></div></div></pre><p>Replace <code>&lt;input&gt;</code> with the path to your input data file, <code>&lt;output&gt;</code> 
with the desired output directory, and <code>&lt;k&gt;</code> with the number of clusters/centroids you want to generate.</p>
<ol start="5"><li>Wait for the execution to complete. The output will be stored in the specified output directory and will contain
the final centroids and the data points assigned to each cluster.</li></ol><h2>Customization</h2><p>This project allows customization
of the K-Means algorithm. You can modify the MapReduce implementation or adjust the parameters to fit your specific needs. Additionally
, you can extend the <code>Point</code> class provided in the project to add more functionality or handle data with different dimensions.</p>
<h2>Contributors</h2>
<ul>
<li><a href="https://github.com/giuliocapecchi" target="_new">Giulio Capecchi</a></li>
<li><a href="https://github.com/fratifederico" target="_new">Federico Frati</a></li>
<li><a href="https://github.com/SteMiche" target="_new">Stefano Micheloni</a></li>
</ul>


<h2>License</h2>
<p>This project is licensed under the <a href="https://github.com/giuliocapecchi/k-means/blob/main/.idea/license.txt" target="_new">MIT License</a>.</p>
<h2>References</h2><ul><li><a href="https://en.wikipedia.org/wiki/K-means_clustering" target="_new">K-Means Clustering</a></li><li>
<a href="https://hadoop.apache.org/" target="_new">Apache Hadoop</a></li></ul></div></div></div>