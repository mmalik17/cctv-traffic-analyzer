# CCTV Traffic Analyzer

Jakarta is one of the most congested cities in the world, not just in Indonesia, with traffic jams costing billions of dollars in lost productivity every year. 
On the good side, several road in Jakarta already has a wide network of publicly accessible CCTV cameras. Recent advances in computer vision now make it entirely feasible to automatically analyze CCTV camera snapshots for vehicle presence, density, and type. 
By combining web scraping, AI inference, structured data storage, and workflow automation through tools like n8n, we can transform raw CCTV streams into actionable, time-stamped traffic insights at minimal cost

This project aims to build an end-to-end automated pipeline that captures the traffic snapshots from publicly accessible Jakarta CCTV cameras. Our project main goals are: <br>
1. Build a fully automated web scraping pipeline that captures timestamped CCTV snapshots from multiple Jakarta traffic camera URLs on a scheduled, recurring basis without any manual intervention <br>
2. Deploy a YOLO-based computer vision model that classifies each image as traffic jam or clear, counts the total number of vehicles, and breaks down vehicle types such as sedans, motorcycles, trucks, and buses <br>
3. Produce a structure dataset stored in Supabase that reveals clear hourly traffic patterns across Jakarta roads, enabling evidence-based insights for commuters and urban planners. <br>



