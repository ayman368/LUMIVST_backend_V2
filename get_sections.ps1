$response = Invoke-WebRequest -Uri 'http://localhost:8000/api/financial-metrics/metric-categories' -UseBasicParsing
$data = $response.Content | ConvertFrom-Json

$sections = @()
foreach ($item in $data) {
    if ($sections -notcontains $item.section) {
        $sections += $item.section
    }
}

Write-Host "Available Sections:"
$sections | Sort-Object | ForEach-Object { 
    Write-Host "  - $_" 
}

Write-Host "`nTotal sections: $($sections.Count)"
