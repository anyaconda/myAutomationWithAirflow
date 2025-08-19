
# 1. Anya HelloWorld DAG Project

This repository contains example Apache Airflow DAGs and related files for learning and testing Airflow workflows.

## Project Structure

- `dags/` - Contains Airflow DAG Python scripts:
  - `helloWorld` dags - Simple Hello World DAGs.


- `tests/` - Contains test scripts:
  - `test_examples.py` - Tests for example DAGs.
  - `test_requirements.txt` - Requirements for running tests.
- `start.sh` - Shell script to start or initialize the project (usage details below).
- `README.md` - Project documentation.

# 2. AnyaIntel project DAG

Added sanitized files for Automated CSV Comparison with Airflow.  

## Key Features
- Securely connects to OneDrive or SharePoint using service credentials  
- List SPSite Document Libraries (aka Drives), list all items in a Drive, list all items in a Drive/Folder  
- Can read all files in a specified SPSite folder

---

# Xtra
## Removed [Original] Getting Started



## License

This project is for educational and demonstration purposes.

## Additional Resources

External:
- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)

---
## Maintainer
**Author:** Anya Chaliotis

---
## Appendix A.

### MS365 for File Management: OneDrive and SharePoint
OneDrive and SharePoint are part of the Microsoft 365 family of products (formerly known as Office 365). This suite includes cloud-based tools and services designed to enhance productivity, collaboration, and data management across organizations.

- OneDrive:  
A personal cloud storage service that allows users to store, sync, and share files securely. It's tightly integrated with Microsoft 365 apps like Word, Excel, and Teams.

- SharePoint:  
A collaborative platform used for document management, intranet portals, and team sites. It enables organizations to manage content, knowledge, and applications to empower teamwork.

Both tools work together to support seamless file sharing and collaboration, especially within Microsoft Teams and Outlook.

## Appendix B. 
###  Microsoft Graph API
List many types of resources in a SharePoint site, including:  
- Document libraries (e.g., "Shared Documents")  
- Folders and files within document libraries  
- SitePages (modern pages, news posts)  - Lists (custom SharePoint lists, including their items)  
- Groups (SharePoint groups and their members)  
-Site users (people with access to the site)  
- Site drives (all document libraries)  
- Site columns and content types  
- Site navigation (menus, links)  
- Site events (calendar events, if enabled)  
- Site permissions (who has access and what level) 

For example, you can use endpoints like: 

- /sites/{site-id}/lists — List all SharePoint lists  
- /sites/{site-id}/drives — List all document libraries  
- /sites/{site-id}/users — List all users with access  
- /sites/{site-id}/columns — List all site columns  
- /sites/{site-id}/contentTypes — List all content types 
