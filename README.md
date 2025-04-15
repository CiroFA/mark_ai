# Programming_project

Introducing Mark – Your Personal AI Investment Assistant

Mark è un assistente virtuale progettato per aiutare investitori e analisti a comprendere e valutare aziende quotate in borsa. La sua intelligenza artificiale è basata su una pipeline di Retrieval-Augmented Generation (RAG), un’architettura che unisce l’accesso a dati esterni strutturati con la generazione di risposte tramite modelli linguistici avanzati come ChatGPT.

Il funzionamento del progetto inizia con la raccolta dei dati finanziari da fonti affidabili. In questa prima fase, Mark si connette alle API di yfinance per scaricare i dati fondamentali e di mercato delle aziende. I dati fondamentali includono variabili chiave come il fatturato, l’utile netto, il rapporto P/E, il margine operativo e altri indicatori economico-finanziari; i dati di mercato comprendono il prezzo corrente delle azioni, la volatilità, i volumi scambiati, il beta e l’andamento storico del titolo. Questi dati vengono salvati nella cartella /data/raw/, organizzati per tipo (fundamentals o market data) e per azienda.

Successivamente, i dati grezzi vengono puliti e trasformati in un formato strutturato più adatto all’elaborazione. In questa fase, gli script rimuovono eventuali anomalie, convertono percentuali e numeri in un formato leggibile, e normalizzano i dati per garantire coerenza tra le diverse fonti. Dopo questa fase di pulizia, le informazioni sono pronte per essere suddivise in “chunk”, ovvero blocchi di testo informativo che rappresentano unità di conoscenza distinte. Ogni chunk può rappresentare, ad esempio, una sintesi dei fondamentali di un’azienda o l’analisi delle sue performance di mercato.

Una volta creati i chunk, questi vengono trasformati in vettori numerici tramite un modello di embedding semantico. Questi vettori, che rappresentano il significato dei testi, vengono indicizzati in un vector store (FAISS), permettendo così a Mark di cercare e recuperare i blocchi di contenuto più rilevanti in base alla domanda dell’utente. Il sistema è costruito per trovare, tra migliaia di chunk, quelli più vicini semanticamente alla richiesta ricevuta.

Quando un utente pone una domanda, ad esempio tramite terminale o futura interfaccia web, Mark la converte in un vettore di embedding e interroga il vector store per trovare i chunk più pertinenti. Questi chunk vengono poi inseriti in un prompt ben strutturato che viene passato al modello linguistico (GPT-3.5 o GPT-4). Il prompt include istruzioni precise: l’assistente deve agire come un analista finanziario esperto, usare solo le informazioni fornite e citare i dati in modo professionale.

Il modello, una volta ricevuto il prompt, genera una risposta completa, ragionata e coerente, restituendola all’utente. La risposta può includere numeri, confronti con il settore, interpretazioni di bilancio o commenti su eventi recenti, a seconda del tipo di domanda e del contenuto recuperato.

La struttura modulare di Mark permette anche di integrare in futuro il web scraping di PDF da fonti istituzionali come Investor Relations, CONSOB o SEC, dove è possibile estrarre relazioni trimestrali, report ESG o altri documenti utili per arricchire il database. Questi PDF verrebbero trattati esattamente come i dati numerici: estrazione, pulizia, chunking, embedding e inserimento nel vector store.

In sintesi, Mark unisce la precisione dei dati finanziari strutturati con la flessibilità e il ragionamento di un modello linguistico, offrendo uno strumento affidabile e aggiornabile per chi investe, analizza o semplicemente vuole capire meglio il mondo della finanza.



Directory:
mark_ai/
├── README.md                       
├── requirements.txt
├── .env
│
├── /data/
│   ├── /raw/fundamentals/
│   ├── /raw/market_data/
│   ├── /pdf_raw/
│   ├── /text_clean/
│   ├── /chunks/
│   └── /index/
│
├── /scripts/
│   ├── download_fundamentals.py
│   ├── download_market_data.py
│   ├── download_pdfs.py
│   ├── extract_text.py
│   ├── clean_text.py
│   ├── chunk_financials.py
│   ├── embed_and_index.py
│   ├── retriever.py
│   ├── llm_wrapper.py
│   └── ask_mark.py                
│
├── /web/
│   └── mark_streamlit_app.py      

Il progetto è strutturato in modo modulare per gestire ogni fase della pipeline, a partire dalla raccolta dei dati fino alla generazione della risposta tramite l’intelligenza artificiale. Di seguito ti spiego passo per passo cosa contengono le singole directory e i programmi all’interno, seguendo l’ordine d’uso che il sistema prevede.

La root del progetto contiene file quali README.md e requirements.txt. Il file README.md serve a documentare il progetto, spiegando la sua finalità (ad esempio “Mark – Your Personal AI Investment Assistant”) e fornendo indicazioni per l’installazione e l’utilizzo. Il file requirements.txt elenca tutte le librerie Python necessarie (come requests, BeautifulSoup, openai, sentence-transformers, faiss, ecc.) per permettere a chiunque di ricreare l’ambiente. Il file .env (o a volte il file api_key.txt nella cartella data) contiene la chiave API per OpenAI e permette di mantenerla separata dal codice, migliorandone la sicurezza.

La directory data ospita tutti i dati che il sistema utilizza e produce. All’interno di data troviamo diverse sottocartelle: • La cartella raw contiene i dati in forma grezza. Qui si trovano due sottocartelle fondamentali: “fundamentals” che contiene i dati fondamentali delle aziende (ad esempio, fatturato, utile netto, P/E ratio, ecc.) scaricati tramite l’API yfinance, e “market_data” che contiene i dati di mercato come prezzi, volumi, beta, storico dei prezzi e simili, sempre scaricati tramite yfinance o altre API dedicate. Questi dati vengono estratti e salvati in formato JSON o CSV senza alcuna elaborazione. • La cartella pdf_raw è dedicata ai PDF scaricati tramite web scraping; nel contesto originario si usava per documenti legali, ma resta utile anche per raccogliere relazioni, report e comunicati stampa che possono essere utili ad un investitore. • La cartella text_clean contiene i file di testo che sono stati ottenuti dai PDF e successivamente “puliti”. Questo significa che il testo grezzo estratto (contenuto nella cartella text_raw, se esistesse) è stato processato per rimuovere numeri di pagina, intestazioni ripetute, spazi inutili, caratteri strani e altri elementi indesiderati, ottenendo così contenuti leggibili e uniformi. • La cartella chunks contiene i dati segmentati in blocchi di testo (“chunk”), che sono pezzi dei documenti o delle descrizioni finanziarie adatti ad essere passati come contesto all’IA. In questi file, per ogni chunk vengono includi non solo il testo ma anche metadati utili, come un identificativo del documento, un numero di chunk, l’origine (ad esempio “Apple Fundamentals”) e, se opportuno, il riferimento a pagina o sezione. • La cartella index contiene il vector store, ovvero i file generati dall’indicizzazione degli embedding; in particolare viene salvato il file faiss.index che contiene i vettori numerici dei chunk, e un file di mapping (come id_to_chunk.json) che serve per collegare ogni vettore al relativo chunk in testo. Questi dati sono usati per eseguire query semantiche efficienti. • Infine, nella directory data c’è anche il file api_key.txt (o i dati presenti nel file .env nella root) che contiene la chiave OpenAI. Questi dati sono necessari per le chiamate all’API ChatGPT.

La directory scripts contiene tutti gli script Python che realizzano le varie fasi della pipeline. Ecco una descrizione di ciascun file, seguendo l’ordine d’uso nel sistema:

download_fundamentals.py: Questo script utilizza la libreria yfinance per scaricare i dati fondamentali delle aziende. Il programma prende in input i ticker delle aziende di interesse (ad esempio AAPL, MSFT, TSLA, ecc.) e salva le informazioni in formato JSON all’interno della cartella data/raw/fundamentals. Il codice si occupa di gestire le chiamate API, salvare i dati grezzi e possibilmente loggare ogni download.

download_market_data.py: Simile al precedente, questo script si occupa invece di scaricare i dati di mercato, come i prezzi attuali, lo storico dei prezzi, i volumi e altre metriche di mercato. Questi dati vengono salvati in data/raw/market_data. Questi file possono essere in formato CSV o JSON e servono a fornire contesto quantitativo per l’analisi.

download_pdfs.py: Questo script gestisce il web scraping per scaricare documenti PDF, ad esempio relazioni trimestrali, comunicati stampa o report ESG, da siti ufficiali o dai siti delle aziende (investor relations). I PDF vengono salvati nella cartella data/pdf_raw. In alternativa, se non si usa molto lo scraping per la demo, questo file potrebbe non essere attivato, ma fa parte della struttura per possibili ulteriori applicazioni.

extract_text.py: Dopo aver scaricato i PDF, questo script utilizza librerie come pdfplumber o PyMuPDF per estrarre il testo grezzo dai documenti. L’output è un file di testo (.txt) per ogni PDF, salvato in data/text_raw (o direttamente in data/text_clean, se il processo di cleaning viene integrato in questo passaggio).

clean_text.py: Questo script prende i file di testo grezzo e li processa per rimuovere elementi indesiderati (numeri di pagina, righe vuote, spazi in eccesso, intestazioni ripetute). Utilizza strumenti come le espressioni regolari (modulo re) e la normalizzazione unicode. Il risultato sono file di testo uniformi e puliti, salvati in data/text_clean.

chunk_financials.py: Questo script prende i file di testo puliti (sia provenienti da documenti scaricati tramite yfinance o dai PDF) e li segmenta in “chunk” ossia blocchi di contenuto limitato a un certo numero di parole o token, solitamente circa 300–500 parole per chunk. Oltre al testo, ad ogni chunk vengono aggiunti metadati come un identificativo del documento, il ticker dell’azienda se applicabile, e altre informazioni utili a identificare la fonte. Il file risultante, che contiene i chunk in formato JSON o JSONL, viene salvato in data/chunks.

embed_and_index.py: In questo script si effettua l’embedding dei chunk. Per fare questo viene utilizzata l’API di OpenAI in particolare il modello text-embedding-ada-002, che trasforma ogni blocco di testo in un vettore numerico che ne rappresenta il significato semantico. Una volta ottenuti gli embedding, questi vengono indicizzati utilizzando FAISS (Facebook AI Similarity Search), una libreria molto efficiente per la ricerca di vettori vicini in spazi ad alta dimensionalità. Il file di indice, tipicamente denominato faiss.index, viene salvato in data/index, insieme a un file di mapping (ad esempio id_to_chunk.json) che collega ogni indice al corrispondente chunk di testo.

retriever.py: Questo script implementa la funzione di retrieval. Riceve in input una domanda che l’utente pone e ne calcola l’embedding (utilizzando, ad esempio, lo stesso modello usato in embed_and_index.py). Con questo vettore, il programma esegue una ricerca nel vector store (FAISS) per recuperare i chunk più simili, solitamente i top 3 o top k chunk. Questo ritorna una lista di stringhe che verranno utilizzate come contesto per generare la risposta.

llm_wrapper.py: Questo file contiene la funzione che costruisce il prompt per il modello ChatGPT e invia la richiesta all’API di OpenAI. Il prompt viene formato includendo un’istruzione che indica al modello di agire come un esperto analista finanziario, e include i chunk estratti come “documenti” su cui basare la risposta, seguito dalla domanda stessa. La funzione invia il prompt utilizzando il modello “gpt-3.5-turbo” (o “gpt-4” se desiderato) e restituisce la risposta generata, che è a sua volta una stringa di testo.

ask_mark.py: Questo è lo script principale che l’utente utilizza in modalità CLI. Quando viene eseguito, chiede all’utente di inserire una domanda. Prende la domanda, utilizza il retriever per ottenere i chunk più rilevanti e poi chiama la funzione in llm_wrapper.py per generare la risposta. Infine, stampa a video la risposta ottenuta, e opzionalmente può mostrare anche i chunk o i riferimenti usati. Questo script mette insieme tutti i componenti precedenti e forma l’interfaccia utente del sistema.

La directory logs contiene file di log, ad esempio per tracciare le chiamate API o registrare i download dei dati, utile per monitorare il funzionamento del sistema e facilitare il debug.

La directory utils raccoglie funzioni di utilità che possono essere chiamate da diversi script, come funzioni per formattare numeri (ad esempio per convertire valori in percentuali o formati finanziari), funzioni per la normalizzazione del testo, o per gestire conversioni specifiche.

Infine, la directory web è riservata per eventuali implementazioni future di un’interfaccia utente web, per esempio basata su Streamlit o FastAPI, che permetteranno di esporre l’assistente in modalità online. Al momento il sistema è impostato come una CLI, ma questa directory è presente per facilitare eventuali upgrade verso un’applicazione web.

Questo percorso rappresenta l’intero flusso di utilizzo del sistema: si parte dal download e dalla raccolta dei dati (sia tramite API come yfinance che tramite web scraping di PDF), si esegue l’estrazione e il cleaning dei contenuti, si segmentano i testi in chunk, si creano gli embedding utilizzando text-embedding-ada-002 e si indicizzano con FAISS, si recuperano i chunk più rilevanti in risposta a una domanda posta dall’utente e infine si passa al modello ChatGPT (tramite llm_wrapper.py) per generare una risposta contestuale e informata.