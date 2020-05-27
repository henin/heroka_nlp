# heroka_nlp 
##### Author: Henin Roland Karkada   <henin.roland@gmail.com>

--------------------------



**heroka_nlp:**   Utility to capture entities for a given sentence based on various NER providers. This utility current supports Spacy, NLTK, AWS Comprehend, Google Cloud Language, allennlp

**Installation:**
> $  git clone <url>
> 
> $ pip install requirements.txt
> 
> $ cd heroka_ner/src/

**Usage:**
> $ from heroka_ner import NER
> 
>Note: Defaults to Spacy NER
>Ex: NER(sentence="John works for IBM technologies and he loves travelling. His favourite destination is New Zealand")
>
> a) Spacy NER
>
> Example 1: NER(process_type="spacy", sentence="John works for IBM technologies and he loves travelling. His favourite destination is New Zealand")
> Output:  
>ENTITIES: {'PERSON': ['John'], 'ORG': ['IBM'], 'GPE': ['New Zealand']}
>
> b) AWS NLP
> Example 2: NER(process_type="aws", sentence="John works for IBM technologies and he loves travelling. His favourite destination is New Zealand")
> Output:  
>ENTITIES: {'PERSON': ['John'], 'ORGANIZATION': ['IBM'], 'LOCATION': ['New Zealand']}
>
> c) GCLOUD NLP
>
> Example 3: NER(process_type="gcloud", sentence="John works for IBM technologies and he loves travelling. His favourite destination is New Zealand")
> Output:  
> {'PERSON': ['John'], 'OTHER': ['technologies'], 'ORGANIZATION': ['IBM'], 'LOCATION': ['New Zealand', 'destination']}
>
> d) NLTK NLP 
>
> Example 4: NER(process_type="nltk", sentence="John works for IBM technologies and he loves travelling. His favourite destination is New Zealand")
> Output:  {'PERSON': ['John'], 'ORGANIZATION': ['IBM'], 'GPE': ['New']}
> 
>
> e) ALLEN  NLP 
>
> Example 5: NER(process_type="allen", sentence="John works for IBM technologies and he loves travelling. His favourite destination is New Zealand")
> Output:  {'PERSON': ['John'], 'ORGANIZATION': ['IBM'], 'GPE': ['New']}
> 
> Usage to run as a script:  python heroka_ner.py <sentence> <spacy|aws|nltk|allen|stanford>"
> 
>python heroka_ner.py "JOHN works for IBM technologies and he loves travelling. His favorite destination is New Zealand" spacy
> 
> Extra Details related to setup for cloud are:
> 1. Create AWS/GCLOUD account
> 2. GCLOUD: store the keys and add this line to bash_rc or bash_profile 
>  ---> export GOOGLE_APPLICATION_CREDENTIALS="/project_path/google_secret_key.json" and source bashrc or bash profile
> 3. For AWS: Go to conf/client.json and add the aws secret key and access key and cloud region as "ap-southeadt-1, ap-south-1" or other regions
> 4. Set up project path in the heroka_nlp/__init__.py if needed.
### Contributing to heroka_nlp
If you want to contribute code to **heroka_nlp**, please take a look at  *CONTRIBUTING.md*.
