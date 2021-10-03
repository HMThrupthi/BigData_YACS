# BD_final_assignment_YACS
Big Data final project - YACS


Execution:
  1. python3 Master.py config.json <scheduling_algorithm>           
  // scheduling algorithm = RANDOM or RR or LL
  2. python3 requests.py <no_of_requests>
  3. python3 Worker.py 4000 1
  4. python3 Worker.py 4001 2
  5. python3 Worker.py 4002 3
  
  A. python3 Analysis.py <scheduling_algorithm>      (Should be ran after master and worker 
  //scheduling_algorithm = RR or RR or LL


   
