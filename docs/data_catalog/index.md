---
icon: material/store-search-outline
title: Data Catalog
---
The data catalog is a centralized hub to keep track of available datasets. It is regularly updated to include new data as it becomes available in any TEF node. If you want access to any dataset, please click "Contact" to reach the owners.

<!-- Search input -->
<div class="search-container">
    <label class="md-search__icon md-icon" for="__search">
        
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M9.5 3A6.5 6.5 0 0 1 16 9.5c0 1.61-.59 3.09-1.56 4.23l.27.27h.79l5 5-1.5 1.5-5-5v-.79l-.27-.27A6.516 6.516 0 0 1 9.5 16 6.5 6.5 0 0 1 3 9.5 6.5 6.5 0 0 1 9.5 3m0 2C7 5 5 7 5 9.5S7 14 9.5 14 14 12 14 9.5 12 5 9.5 5Z"></path></svg>
        
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M20 11v2H8l5.5 5.5-1.42 1.42L4.16 12l7.92-7.92L13.5 5.5 8 11h12Z"></path></svg>
    </label>
    <input type="text" id="searchInput" placeholder="Search for names..." />
</div>

| Dataset | Super Node | TEF Node | Owner | Follows SDM | Data Model | Get Access |
| --------| ---------- | -------- | ----- | ----------- | ---------- | ---------- |
| WasteContainer      | Southern   | València City | València City Council | Partially   | [See specification](https://gitlab.com/vlci-public/models-dades/wastemanagement/-/blob/main/WasteContainer/spec.md)     | [Contact owner](https://valencia.opendatasoft.com/pages/home/) |
| TrafficFlowObserved | Southern   | València City | València City Council | Partially   | [See specification](https://gitlab.com/vlci-public/models-dades/environment/-/blob/main/AirQualityObserved/spec.md)     | [Contact owner](https://valencia.opendatasoft.com/pages/home/) |
| AirQualityObserved  | Southern   | València City | València City Council | Partially   | [See specification](https://gitlab.com/vlci-public/models-dades/transportation/-/blob/main/TrafficFlowObserved/spec.md) | [Contact owner](https://valencia.opendatasoft.com/pages/home/) |
| Weather Forecast | SOUTH | VALENCIA | Valencia City |  |  | https://valencia.opendatasoft.com/pages/home/ |
| Waste Container | SOUTH | VALENCIA | Valencia City |  |  | https://valencia.opendatasoft.com/pages/home/ |

<script>
document.addEventListener("DOMContentLoaded", function() {
    // Hide sidebar
    document.querySelector('.md-sidebar--secondary').style.display = 'none';

    // filter
    const searchInput = document.getElementById("searchInput");
    const table = document.querySelector("table");

    searchInput.addEventListener("keyup", function() {
        const filter = searchInput.value.toLowerCase();
        const rows = table.getElementsByTagName("tr");

        for (let i = 1; i < rows.length; i++) {
            const cells = rows[i].getElementsByTagName("td");
            let found = false;

            for (let j = 0; j < cells.length; j++) {
                if (cells[j].textContent.toLowerCase().indexOf(filter) > -1) {
                    found = true;
                    break;
                }
            }

            rows[i].style.display = found ? "" : "none";
        }
    });
});
</script>