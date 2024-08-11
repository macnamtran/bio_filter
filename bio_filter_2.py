import pandas as pd
import re
from fuzzywuzzy import fuzz

# Load the CSV files
#profiles_path = "C:/py/pythonProject/profile_bio.csv"
profiles_path = "C:\py\pythonProject\instagram_profile_scraper_results - instagram_profile_scraper_results.csv.csv"
job_positions_path = "C:\py\pythonProject\job_positions_final_updated.csv"
cities_path = "C:\py\pythonProject\Cities.csv"
countries_path = "C:\py\pythonProject\Countries.csv"

# Read job positions and profiles data
job_positions_df = pd.read_csv(job_positions_path)
profiles_df = pd.read_csv(profiles_path)
cities_df = pd.read_csv(cities_path)
countries_df = pd.read_csv(countries_path)

# Extract job positions into a list
job_positions_list = job_positions_df['Job Position'].tolist()
cities_list = cities_df['City'].tolist()
countries_list = countries_df['Country'].tolist()

# Define abbreviations for job positions
abbreviations = {
    "software developer": ["software dev", "sw dev", "lead software developer"],
    "mobile developer": ["mobile dev", "lead mobile developer", "mobile application developer"],
    "user experience designer": ["ux designer", "lead ux designer"],
    "user interface designer": ["ui designer"],
    "designer": ["designer"],
    "full stack developer": ["full stack dev", "fullstack developer"],
    "front end developer": ["front end dev", "frontend developer"],
    "back end developer": ["back end dev", "backend developer"],
    "devops engineer": ["devops eng", "devops"],
    "data scientist": ["data sci", "ds"],
    "project manager": ["pm"],
    "chief executive officer": ["ceo"],
    "chief technology officer": ["cto"],
    "chief financial officer": ["cfo"],
    "chief operating officer": ["coo"],
    "photographer": ["photog"],
    "film maker": ["filmmaker"],
    "comedian": ["comedian", "stand-up comedian", "stand up comedian"],
    "writer": ["writer"],
    "author": ["author"],
    "editor": ["editor"],
    "engineer": ["eng", "engr"],
    "entrepreneur": ["entrepreneur"],
    "investor": ["investor"],
    "athlete": ["athlete"],
    "founder": ["founder"],
    "co-founder": ["co-founder"],
    "associate director": ["assoc dir"],
    "chief analytics officer": ["cao"],
    "chief administrative officer": ["cao"],
    "chief data officer": ["cdo"],
    "chief human resources officer": ["chro"],
    "chief information officer": ["cio"],
    "chief legal officer": ["clo"],
    "chief marketing officer": ["cmo"],
    "chief product officer": ["cpo"],
    "chief security officer": ["cso"],
    "Vice President of Sales"
    "Vice President of Marketing": ["vp of marketing"],
    "Vice President of Operations": ["vp of operations"],
    "Vice President of Engineering": ["vp of engineering"],
    "Vice President of Human Resources": ["vp of hr"],
    "Vice President of Finance": ["vp of finance"],
    "Vice President of Product": ["vp of product"],
    "Vice President of Business Development": ["vp of bd"],
    "Vice President of Customer Success": ["vp of customer success"],
    "Vice President of Public Relations": ["vp of public relations"],
    "Vice President of Communications": ["vp of communications"],
    "Director of Business Development": ["director of business development"],
    "Director of Marketing": ["director of marketing"],
    "Director of Sales": ["director of sales"],
    "Director of Operations": ["director of operations"],
    "Director of Engineering": ["director of engineering"],
    "Director of Human Resources": ["director of hr"],
    "Director of Finance": ["director of finance"],
    "Director of Product Management": ["director of product management"],
    "Director of Customer Success": ["director of customer success"],
    "Head of Business Development": ["head of business development", "head of biz development", "head of biz dev"],
    "Head of Product Management": ["head of product management"],
    "Head of Marketing": ["head of marketing"],
    "Head of Digital Marketing": ["head of digital marketing"],
    "Head of Sales": ["head of sales"],
    "Head of Operations": ["head of operations"],
    "Head of Human Resources": ["head of hr"],
    "Head of Financial Planning": ["head of financial planning"],
    "Head of Corporate Strategy": ["head of corporate strategy"],
    "Head of Compliance": ["head of compliance"],
    "Head of Risk Management": ["head of risk management"],
    "Business Development Manager": ["business development manager"],
    "Marketing Manager": ["marketing manager"],
    "Sales Manager": ["sales manager"],
    "Office Manager": ["office manager"],
    "Operations Manager": ["operations manager"],
    "Finance Manager": ["finance manager"],
    "Engineering Manager": ["engineering manager"],
    "Human Resources Manager": ["human resources manager"],
    "Product Manager": ["product manager"],
    "Customer Success Manager": ["customer success manager"],
    "Public Relations Manager": ["public relations manager"],
    "Communications Manager": ["communications manager"],
    "Content Marketer": ["content marketer"],
    "Head of Inbound Marketing": ["head of inbound marketing"],
    "Head of Paid Media": ["head of paid media"],
    "Head of Paid Media Marketing": ["head of paid media marketing"],
    "director": ["director"],
    "executive director": ["ed"],
    "executive vice president": ["evp"],
    "managing director": ["md"],
    "managing partner": ["mp"],
    "president": ["president"],
    "sergeant": ["sgt"],
    "supervisor": ["supr", "supe"],
    "vice chancellor": ["vc"],
    "vice president": ["vp"],
    "vice president of talent acquisition": ["vp of talent acquisition"],
    "vice president of talent management": ["vp of talent management"],
    "manager": ["mgr", "mgr", 'manager'],
    "account manager": ["am", 'account manager'],
    "area manager": ["am", 'area manager'],
    "area business manager": ["amb", 'area business manager'],
    "accounting manager": ["acct mgr", 'accounting manager'],
    "executive account manager": ["eam", 'executive account manager'],
    "human resources manager": ["hrm", 'human resources manager'],
    "senior manager": ["sr mgr", 'senior manager'],
    "territory manager": ["ter mgr", 'territory manager'],
    "administrative assistant": ["admin asst"],
    "assistant": ["asst", "assistant"],
    "executive assistant": ["exec asst", "executive assistant"],
    "personal assistant": ["pa"],
    "virtual assistant": ["va"],
    "accounting assistant": ["aa"],
    "assistant area sales advisor": ["aasa"],
    "assistant director": ["ad", "assistant director"],
    "assistant vice president": ["avp", "assistant vice president"],
    "application engineer": ["ae"],
    "aeronautical engineer": ["aero eng"],
    "chief engineer": ["ce", "c/e", "chief eng", "chief engr", "chief engineer"],
    "chemical engineer": ["chem engr", "chemical engr", "chem eng", "chemical engineer"],
    "cloud ops engineer": ["cloud ops eng"],
    "computer network engineer": ["cne"],
    "computer engineer": ["comp engr"],
    "electrical engineer": ["electr eng"],
    "environmental engineer": ["environ engr"],
    "junior engineer": ["jr engr"],
    "manufacturing engineer": ["manuf eng"],
    "mechanical engineer": ["mech engr"],
    "professional engineer": ["prof engr"],
    "senior application engineer": ["sr ae"],
    "structural engineer": ["struct eng"],
    "software engineer": ["swe"],
    "user interface engineer": ["ui eng"],
    "developer": ["dev"],
    "front-end developer": ["fe developer"],
    "javascript developer": ["js developer"],
    "account executive": ["ae"],
    "business development manager": ["bdm"],
    "business development representative": ["bdr"],
    "sales representative": ["sales rep"],
    "sales development representative": ["sdr"],
    "art director": ["art dir", "art director"],
    "creative director": ["creative dir", "creative director"],
    "administrator": ["admin"],
    "advisor": ["adv", "advisor"],
    "agent": ["agent"],
    "analyst": ["anlst"],
    "apprentice": ["apr", "aapr"],
    "associate": ["assoc"],
    "associate professor": ["assoc prof"],
    "attendant": ["attd"],
    "certified financial planner": ["cfp"],
    "chancellor": ["chanc"],
    "coordinator": ["coord"],
    "certified public accountant": ["cpa"],
    "customer success manager": ["csm"],
    "customer service representative": ["csr"],
    "data analyst": ["da"],
    "electrician": ["electrician"],
    "general counsel": ["gc"],
    "human resources business partner": ["human resources business partner"],
    "inspector": ["inspector"],
    "instructor": ["instructor"],
    "interim": ["int"],
    "information technology": ["it"],
    "lecturer": ["lec"],
    "machinist": ["mach"],
    "mechanic": ["mech"],
    "marketing": ["mkg", "mkt", "mktg"],
    "operator": ["oper"],
    "product manager": ["pm"],
    "principal": ["prin"],
    "public relations officer": ["pro"],
    "professor": ["prof"],
    "programmer": ["programmer"],
    "representative": ["rep"],
    "ruby on rails developer": ["ror dev"],
    "quality assurance analyst": ["qa analyst"],
    "sales associate": ["sales assoc"],
    "social media specialist": ["sm spec"],
    "specialist": ["spec", "specialist"],
    "senior": ["sr"],
    "technician": ["technician"],
    "trainee": ["trainee"],
    "trainer": ["trainer"],
    "arbitrator": ["arbitrator"],
    "attorney general": ["attorney general"],
    "baron": ["b."],
    "chief baron": ["c.b.", "chief baron"],
    "chief justice": ["c.j.", "chief justice"],
    "judge": ["judge"],
    "magistrate": ["magistrate"],
    "mediator": ["mediator"],
    "senator": ["senator"],
    "advanced practice registered nurse": ["aprn"],
    "family nurse practitioner": ["aprn-fnp"],
    "adult nurse practitioner": ["aprn-anp"],
    "certified adult nurse practitioner": ["canp"],
    "adult nurse practitioner – board certified": ["anp-bc"],
    "adult-gerontology acute care nurse practitioner": ["aprn-agacnp"],
    "psychiatric nurse practitioner": ["aprn-pnp"],
    "certified nurse midwife": ["aprn-cnm"],
    "acute care nurse practitioner – board certified": ["acnp-bc"],
    "aids certified registered nurse": ["acrn"],
    "advanced legal nurse consultant": ["alnc"],
    "certified legal nurse consultant": ["clnc"],
    "advanced oncology certified nurse": ["aocn"],
    "advanced oncology certified nurse practitioner": ["aocnp"],
    "advanced oncology certified clinical nurse specialist": ["aocns"],
    "advanced practice nurse": ["apn"],
    "advanced practice nurse prescriber": ["apnp"],
    "advanced registered nurse practitioner": ["arnp"],
    "certified ambulatory post-anesthesia nurse": ["capa"],
    "certified ambulatory perianesthesia nurse": ["capa"],
    "certified addiction registered nurse": ["carn"],
    "certified continence care nurse": ["cccn"],
    "certified clinical nurse specialist": ["ccns"],
    "critical care nurse specialist": ["ccns"],
    "critical care registered nurse": ["ccrn"],
    "certified clinical transplant nurse": ["cctn"],
    "certified critical care transportation nurse": ["cctrn"],
    "certified developmental disabilities nurse": ["cddn"],
    "certified dialysis nurse": ["cdn"],
    "certified director of nursing administration/long term care": ["cdona/ltc"],
    "certified emergency nurse": ["cen"],
    "certified enterostomal therapy nurse": ["cetn"],
    "certified foot care nurse": ["cfcn"],
    "certified forensic nurse": ["cfn"],
    "certified family nurse practitioner": ["cfnp"],
    "certified flight registered nurse": ["cfrn"],
    "certified gastroenterology nurse": ["cgn"],
    "certified gastroenterology registered nurse": ["cgrn"],
    "certified hemodialysis nurse": ["chn"],
    "certified hospice and palliative nurse": ["chpn"],
    "certified hyperbaric registered nurse": ["chrn"],
    "certified licensed practitioner nursing, intravenous": ["clpni"],
    "certified managed care nurse": ["cmcn"],
    "certified medical-surgical registered nurse": ["cmsrn"],
    "certified in nursing administration": ["cna"],
    "certified nursing director of long-term care": ["cndltc"],
    "clinical nursing intern": ["cni"],
    "certified nurse life care planner": ["cnlcp"],
    "certified nephrology nurse": ["cnn"],
    "certified neonatal nurse practitioner": ["cnnp"],
    "chief nursing officer": ["cno"],
    "certified nurse practitioner": ["cnp"],
    "community nurse practitioner": ["cnp"],
    "certified neuroscience registered nurse": ["cnrn"],
    "clinical nurse specialist": ["cns"],
    "certified nutrition support nurse": ["cnsn"],
    "certified ostomy care nurse": ["cocn"],
    "certified occupational health nurse": ["cohn"],
    "certified occupational health nurse – specialist": ["cohn-s"],
    "certified occupational health nurse – specialist/case manager": ["cohn-s/cm"],
    "certified otorhinolaryngology nurse": ["corln"],
    "certified operating room nurse": ["corn"],
    "certified post anesthesia nurse": ["cpan"],
    "certified peritoneal dialysis nurse": ["cpsn"],
    "certified pediatric nurse": ["cpn"],
    "certified pediatric nurse associate": ["cpna"],
    "certified practical nurse, long-term care": ["cpnl"],
    "certified pediatric nurse practitioner": ["cpnp"],
    "certified pediatric oncology nurse": ["cpon"],
    "certified plastic surgical nurse": ["cpsn"],
    "certified radiologic nurse": ["crn"],
    "certified registered nurse anesthetist": ["crna"],
    "certified registered hospice nurse": ["crnh"],
    "certified registered nurse infusion": ["crni"],
    "certified registered nurse intravenous": ["crni"],
    "certified registered nurse, long-term care": ["crnl"],
    "certified registered nurse in ophthalmology": ["crno"],
    "certified registered nurse practitioner": ["crnp"],
    "certified rehabilitation registered nurse": ["crrn"],
    "certified rehabilitation registered nurse – advanced": ["crrn-a"],
    "certified school nurse": ["csn"],
    "certified transcultural nurse": ["ctn"],
    "certified transport registered nurse": ["ctrn"],
    "certified urologic clinical nurse specialist": ["cucns"],
    "certified urologic nurse practitioner": ["cunp"],
    "certified urologic registered nurse": ["curn"],
    "certified vascular nurse": ["cvn"],
    "certified wound care nurse": ["cwcn"],
    "certified wound, ostomy, continence nurse": ["cwocn"],
    "dermatology nurse certified": ["dnc"],
    "director of nursing": ["don"],
    "enrolled nurse": ["en"],
    "enrolled nurse practitioner": ["enp"],
    "fellow of the american academy of nursing": ["faan"],
    "fellow of the academy of emergency nursing": ["faen"],
    "immunisation program nurse": ["ipn"],
    "legal nurse consultant": ["lnc"],
    "legal nurse consultant certified": ["lncc"],
    "licensed school nurse": ["lsn"],
    "licensed vocational nurse": ["lvn"],
    "licensed visiting nurse": ["lvn"],
    "mental health nurse": ["mhn"],
    "mobile intensive care nurse": ["micn"],
    "master of nursing": ["mn"],
    "master of science in nursing": ["msn"],
    "national certified school nurse": ["ncsn"],
    "nurse executive-board certified": ["ne-bc"],
    "nurse executive advanced-board certified": ["nea-bc"],
    "neonatal intensive care nurse": ["nic"],
    "nurse massage therapist": ["nmt"],
    "neonatal nurse practitioner": ["nnp"],
    "nurse practitioner": ["np"],
    "nurse practitioner, certified": ["npc"],
    "nurse practitioner, psychiatric": ["npp"],
    "oncology certified nurse": ["ocn"],
    "obstetrics & gynecology nurse practitioner": ["ognp"],
    "occupational health nursing clinical specialist": ["ohncs"],
    "orthopedic nurse certified": ["onc"],
    "progressive care certified nurse": ["pccn"],
    "pediatric clinical nurse specialist": ["pcns"],
    "public health nurse": ["phn"],
    "pre-hospital registered nurse": ["phrn"],
    "psychiatric mental health clinical nurse specialist": ["pmhcns"],
    "psychiatric mental health nurse practitioner": ["pmhnp"],
    "pediatric nurse practitioner": ["pnp"],
    "rural and isolated practice registered nurses": ["riprn"],
    "registered nurse": ["registered nurse"],
    "registered nurse, board certified": ["rn-bc"],
    "registered nurse anesthetist": ["rna"],
    "registered nurse certified": ["rnc"],
    "registered nurse clinical specialist": ["rncs"],
    "registered nurse certified specialist": ["rncs"],
    "registered nurse first assistant": ["rnfa"],
    "registered nurse practitioner": ["rnp"],
    "registered practical nurse": ["rpn"],
    "sexual assault nurse examiner-adult/adolescent": ["sane-a"],
    "sexual assault nurse examiner-pediatric": ["sane-"],
    "state enrolled nurse": ["sen"],
    "sexual and reproductive health nurse": ["shn"],
    "student nurse": ["sn"],
    "student professional nurse": ["spn"],
    "telephone nursing practitioner": ["tnp"],
    "trauma nurse specialist": ["tns"],
    "women’s health nurse practitioner": ["whnp"],
    "wound, ostomy, continence nurses": ["wocn"],
    "certified specialist in poison information": ["c-spi"],
    "doctor of surgery": ["chd"],
    "doctor of podiatric medicine": ["dpm"],
    "doctor of medical education": ["dme"],
    "doctor of medical science": ["dmsc"],
    "doctor of medical technology": ["dmt"],
    "doctor of nursing": ["dn"],
    "doctor of nursing education": ["dne"],
    "doctor of nursing science": ["dns"],
    "doctor of osteopathy": ["do"],
    "doctor of ocular science": ["dos"],
    "doctor of public health nursing": ["dphn"],
    "doctor of nursing practice": ["drnp"],
    "doctor of public health": ["drph"],
    "doctor of social work": ["dsw"],
    "licentiate of the royal college of surgeons": ["lrcs"],
    "medicinae baccalaureus, baccalaureus chirurgiae (latin: bachelor of medicine, bachelor of surgery)": ["mbbs"],
    "medical doctor": ["md"],
    "member of the royal college of physicians": ["mrcp"],
    "member of the royal college of surgeons": ["mrcs"],
    "master of surgery": ["msurg", "ms", "msc"],
    "nuclear cardiology technologist": ["nct"],
    "doctor of naturopathy": ["nd"],
    "doctor of optometry": ["od"],
    "physician assistant": ["pa", "pa-c"],
    "certified laboratory assistant": ["cla"],
    "certified medical assistant": ["cma"],
    "certified medical assistant – administrative": ["cma-a"],
    "certified medical assistant – clinical": ["cma-c"],
    "certified nursing assistant": ["cna"],
    "certified nursing assistant – advanced": ["cna-a"],
    "certified ophthalmic medical assistant": ["coma"],
    "certified occupational therapy assistant": ["cota"],
    "certified registered nurse first assistant": ["crnfa"],
    "master of physician assistant studies": ["mpas"],
    "medical records librarian": ["mrl"],
    "medical technologist assistant": ["mta"],
    "occupational therapist assistant": ["ota"],
    "registered medical assistant": ["rma"],
    "certified dental assistant": ["cda"],
    "dental assistant": ["da"],
    "doctor of dental surgery": ["dds"],
    "doctor of dental science": ["ddsc"],
    "doctor of dental medicine": ["dmd"],
    "fellow of the american college of dentists": ["facd"],
    "fellow of the academy of general dentistry": ["fagd"],
    "fellow in dental surgery": ["fds"],
    "master of dental surgery": ["mds"],
    "master of science in dentistry": ["msd"],
    "registered dental assistant": ["rda"],
    "registered dental hygienist": ["rdh"],
    "pharmacist": ["ph", "rph", "phar", "pharm"],
    "doctor of pharmacy": ["pharm.d.", "pharmd"],
    "lead": [
        "lead engineer"
        "lead software developer",
        "lead mobile developer",
        "lead user experience designer",
        "lead user interface designer",
        "lead designer",
        "lead full stack developer",
        "lead front end developer",
        "lead back end developer",
        "lead devops engineer",
        "lead data scientist",
        "lead project manager",
        "lead photographer",
        "lead film maker",
        "lead comedian",
        "lead writer",
        "lead author",
        "lead editor",
        "lead engineer",
        "lead entrepreneur",
        "lead investor",
        "lead athlete",
        "lead associate director",
        "lead business development manager",
        "lead content marketer",
        "lead inbound marketing",
        "lead paid media",
        "lead paid media marketing"
    ]
}


# Flatten abbreviations list for comparison
abbreviation_list = [abbr for sublist in abbreviations.values() for abbr in sublist]

# Ensure case insensitivity
job_positions_list = [position.lower() for position in job_positions_list]
abbreviation_list = [abbr.lower() for abbr in abbreviation_list]
cities_list = [city.lower() for city in cities_list]
countries_list = [country.lower() for country in countries_list]

# Define function to extract companies from bio
def extract_all_companies_and_emails(bio):
    if isinstance(bio, str):
        # Extract email addresses
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b', bio)
        # Remove email addresses before extracting companies
        bio_no_emails = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b', '', bio)
        companies = re.findall(r'@(\w+)', bio_no_emails)
        return companies, emails
    return [], []

# Define function to extract job positions from bio
def extract_all_job_positions(bio):
    if isinstance(bio, str):
        bio_lower = bio.lower()
        found_positions = set()
        words = re.findall(r'\w+', bio_lower)

        # Exclude specific short words that shouldn't be matched
        excluded_words = {"co"}

        # Check for matches in the abbreviation list first
        for abbr in abbreviation_list:
            if re.search(r'\b' + re.escape(abbr) + r'\b', bio_lower):
                found_positions.add(abbr)

        # Check against the full job positions list
        for position in job_positions_list:
            if re.search(r'\b' + re.escape(position) + r'\b', bio_lower) and position not in excluded_words:
                found_positions.add(position)
            else:
                # Fuzzy match with stricter conditions
                for word in words:
                    if len(word) > 3 and fuzz.ratio(position, word) > 90 and word not in excluded_words:
                        found_positions.add(position)

        # Create a final set of positions, preferring more specific ones
        final_positions = set()
        for pos in found_positions:
            is_specific = True
            for other_pos in found_positions:
                if pos != other_pos and pos in other_pos:
                    is_specific = False
                    break
            if is_specific:
                final_positions.add(pos)

        return list(final_positions)  # Ensure unique values
    return []

# Define function to extract cities from bio
def extract_all_cities(bio):
    if isinstance(bio, str):
        found_cities = set()
        bio_lower = bio.lower()
        for city in cities_list:
            if re.search(r'\b' + re.escape(city) + r'\b', bio_lower):
                found_cities.add(city)
        return list(found_cities)
    return []

# Define function to extract countries from bio
def extract_all_countries(bio):
    if isinstance(bio, str):
        found_countries = set()
        bio_lower = bio.lower()
        for country in countries_list:
            if re.search(r'\b' + re.escape(country) + r'\b', bio_lower):
                found_countries.add(country)
        return list(found_countries)
    return []

# Apply the functions to the bio column
profiles_df[['Companies', 'Emails']] = profiles_df['bio'].apply(lambda bio: pd.Series(extract_all_companies_and_emails(bio)))
profiles_df['Positions'] = profiles_df['bio'].apply(extract_all_job_positions)
profiles_df['Cities'] = profiles_df['bio'].apply(extract_all_cities)
profiles_df['Countries'] = profiles_df['bio'].apply(extract_all_countries)

# Save the updated DataFrame to a new CSV file
output_path = "C:/py/pythonProject/extracted_companies_and_positions_cities_countries.csv"
profiles_df.to_csv(output_path, index=False)

print(f"Updated file saved to {output_path}")
