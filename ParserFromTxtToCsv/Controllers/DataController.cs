using Microsoft.AspNetCore.Mvc;
using System.Data.Odbc;

namespace BabyNi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : Controller
    {

        DataAggregationService dataAggregationService = new DataAggregationService();
        


        [HttpGet("hourly")]
        public ActionResult<List<AggregatedDataDto>> GetHourlyData()
        {
            return Ok(dataAggregationService.GetAggregatedData("TRANS_MW_AGG_SLOT_HOURLY"));
        }

        [HttpGet("daily")]
        public ActionResult<List<AggregatedDataDto>> GetDailyData()
        {
            return Ok(dataAggregationService.GetAggregatedData("TRANS_MW_AGG_SLOT_DAILY"));
        }

        
    }
}
