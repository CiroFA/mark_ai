Introducing Mark – Your Personal AI Investment Assistant

Mark è un assistente virtuale progettato per aiutare investitori e analisti a comprendere e valutare aziende quotate in borsa. La sua intelligenza artificiale è basata su una pipeline modulare che unisce dati numerici (quantitativi) e dati testuali (qualitativi), combinando query SQL e Retrieval-Augmented Generation (RAG) con modelli linguistici avanzati come GPT-4.5.

Il funzionamento del progetto inizia con la raccolta dei dati finanziari da fonti affidabili. In questa fase, Mark si connette all’API yfinance per scaricare i dati fondamentali e di mercato delle aziende. I dati fondamentali includono variabili chiave come il P/E ratio, il margine operativo, il ROE e l’utile per azione; i dati di mercato comprendono il prezzo storico delle azioni, i volumi scambiati, il beta e altri indicatori. Questi dati vengono salvati in un database relazionale SQL, in tre tabelle principali: companies, market_data, e fundamentals.

La struttura del database viene inizialmente creata tramite lo script create_db.py, e successivamente popolata e aggiornata tramite update_db.py, che può essere eseguito periodicamente anche tramite crontab. Le funzioni di aggiornamento evitano i duplicati e assicurano che ogni riga abbia un timestamp coerente con la fonte originale.

Parallelamente, Mark acquisisce e gestisce anche contenuti testuali ufficiali, come i report 10-K e 10-Q pubblicati su EDGAR dalla SEC. Questi vengono scaricati in formato HTML tramite scraping automatizzato, salvati in /data/pdf_raw/, e processati tramite una serie di script modulari. Lo script extract_text.py estrae il testo leggibile dall’HTML usando BeautifulSoup; clean_text.py rimuove intestazioni ripetute, numeri di pagina e rumore; chunk_text.py segmenta il contenuto in blocchi semantici (chunk) da circa 800 token ciascuno, pronti per l’analisi semantica. Ogni chunk viene poi trasformato in un vettore numerico tramite text-embedding-ada-002 di OpenAI, e indicizzato in un vector store FAISS per consentire ricerche efficienti.

Quando un utente pone una domanda, Mark attiva una pipeline decisionale. Il modulo classify_question.py analizza il testo della domanda e la classifica come numeric, text, o hybrid. Se la domanda è numerica (es. “Qual era il P/E di Apple nel 2020?”), viene costruita una query SQL mirata tramite answer_numerical_query.py. Se è testuale (es. “Quali sono i principali rischi dichiarati da Tesla nel suo 10-K?”), viene calcolato l’embedding della domanda, eseguita la ricerca nei chunk tramite FAISS, e recuperato il contesto testuale. Se la domanda è ibrida (es. “Come è cambiato il margine operativo di Microsoft e quali motivazioni sono state fornite nel 10-K?”), Mark unisce i due percorsi in un’unica risposta.

La risposta finale viene costruita dal modulo llm_wrapper.py, che riceve i chunk rilevanti o i risultati SQL e crea un prompt strutturato da inviare al modello GPT-4.5. Il prompt include istruzioni chiare (es. "Non rispondere se l’informazione non è presente nei documenti forniti") e presenta il contesto in modo controllato. La risposta generata viene restituita all’utente, arricchita da riferimenti ai documenti o ai dati originali, mantenendo un tono professionale.

Per garantire l'affidabilità, Mark implementa tre livelli di difesa contro le hallucinations: 1) istruzioni esplicite nel prompt a non inventare nulla; 2) soglia di similarità (es. 0.5) sotto la quale Mark dichiara di non sapere; 3) fallback propositivi tra 0.5 e 0.7, in cui Mark propone contenuti correlati ("non so X, ma posso dirti Y"). Questa architettura rende Mark un sistema robusto e scalabile, capace di distinguere ciò che sa da ciò che non sa.

In sintesi, Mark unisce la precisione dei dati finanziari strutturati con la flessibilità del linguaggio naturale, offrendo uno strumento intelligente, aggiornabile e trasparente per analizzare il mondo delle aziende quotate.

Directory: 
mark_ai/ 
 ├── README.md
 ├── requirements.txt 
 ├── environment.yml │ 
  /data/ │ 
 ├── /db/ # Database relazionale SQL │ 
 ├── /raw/ # (opzionale) dati non strutturati │
 ├── /pdf_raw/ # HTML dei filing SEC (EDGAR) │ 
 ├── /text_clean/ # Testi puliti e uniformati │ 
 ├── /chunks/ # Blocchi semantici (chunk) │ 
 └── /index/ # Vector store FAISS + mapping │ 
 /scripts/ │ 
 ├── create_db.py # Crea struttura SQL │ 
 ├── update_db.py # Aggiorna dati da yfinance │ 
 ├── download_edgar_reports.py # Scarica filing da EDGAR │
 ├── extract_text.py # Estrazione testo HTML │ 
 ├── clean_text.py # Pulizia del testo │ 
 ├── chunk_text.py # Divisione in chunk │
 ├── embed_and_index.py # Embedding + indicizzazione FAISS │
 ├── classify_question.py # Classificazione domanda │
 ├── answer_numerical_query.py # Costruzione query SQL │ 
 ├── retriever.py # Retrieval semantico │ 
 ├── llm_wrapper.py # Prompt + chiamata GPT │ 
  └── ask_mark.py # Entry point CLI │ 
  /web/ │
  └── mark_streamlit_app.py # UI web (prototipo futuro)

Questo progetto è stato progettato per scalare nel tempo, integrando in futuro nuove fonti informative (es. ESG, Investor Relations), supportando diversi modelli linguistici, e fornendo risposte sempre più affidabili attraverso un controllo rigoroso delle fonti utilizzate.